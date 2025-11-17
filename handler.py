import runpod
from runpod.serverless.utils import rp_upload
import os
import shutil
import websocket
import base64
import json
import uuid
import logging
import urllib.request
import urllib.parse
import binascii # For Base64 error handling
import subprocess
import time
# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


server_address = os.getenv('SERVER_ADDRESS', '127.0.0.1')
client_id = str(uuid.uuid4())
def to_nearest_multiple_of_16(value):
    """Adjust the given value to the nearest multiple of 16, ensuring minimum of 16"""
    try:
        numeric_value = float(value)
    except Exception:
        raise Exception(f"width/height value is not a number: {value}")
    adjusted = int(round(numeric_value / 16.0) * 16)
    if adjusted < 16:
        adjusted = 16
    return adjusted
def process_input(input_data, temp_dir, output_filename, input_type):
    """Process input data and return file path"""
    if input_type == "path":
        # If it's a path, return as-is
        logger.info(f"📁 Processing path input: {input_data}")
        return input_data
    elif input_type == "url":
        # If it's a URL, download it
        logger.info(f"🌐 Processing URL input: {input_data}")
        os.makedirs(temp_dir, exist_ok=True)
        file_path = os.path.abspath(os.path.join(temp_dir, output_filename))
        return download_file_from_url(input_data, file_path)
    elif input_type == "base64":
        # If it's Base64, decode and save
        logger.info(f"🔢 Processing Base64 input")
        return save_base64_to_file(input_data, temp_dir, output_filename)
    else:
        raise Exception(f"Unsupported input type: {input_type}")

        
def download_file_from_url(url, output_path):
    """Download file from URL"""
    try:
        # Download file using wget
        result = subprocess.run([
            'wget', '-O', output_path, '--no-verbose', url
        ], capture_output=True, text=True)

        if result.returncode == 0:
            logger.info(f"✅ Successfully downloaded file from URL: {url} -> {output_path}")
            return output_path
        else:
            logger.error(f"❌ wget download failed: {result.stderr}")
            raise Exception(f"URL download failed: {result.stderr}")
    except subprocess.TimeoutExpired:
        logger.error("❌ Download timeout")
        raise Exception("Download timeout")
    except Exception as e:
        logger.error(f"❌ Error during download: {e}")
        raise Exception(f"Error during download: {e}")


def save_base64_to_file(base64_data, temp_dir, output_filename):
    """Save Base64 data to file"""
    try:
        # Decode Base64 string
        decoded_data = base64.b64decode(base64_data)

        # Create directory if it doesn't exist
        os.makedirs(temp_dir, exist_ok=True)

        # Save to file
        file_path = os.path.abspath(os.path.join(temp_dir, output_filename))
        with open(file_path, 'wb') as f:
            f.write(decoded_data)

        logger.info(f"✅ Saved Base64 input to file '{file_path}'.")
        return file_path
    except (binascii.Error, ValueError) as e:
        logger.error(f"❌ Base64 decoding failed: {e}")
        raise Exception(f"Base64 decoding failed: {e}")
    
def queue_prompt(prompt):
    url = f"http://{server_address}:8188/prompt"
    logger.info(f"Queueing prompt to: {url}")
    p = {"prompt": prompt, "client_id": client_id}
    data = json.dumps(p).encode('utf-8')
    req = urllib.request.Request(url, data=data)
    return json.loads(urllib.request.urlopen(req).read())

def get_image(filename, subfolder, folder_type):
    url = f"http://{server_address}:8188/view"
    logger.info(f"Getting image from: {url}")
    data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
    url_values = urllib.parse.urlencode(data)
    with urllib.request.urlopen(f"{url}?{url_values}") as response:
        return response.read()

def get_history(prompt_id):
    url = f"http://{server_address}:8188/history/{prompt_id}"
    logger.info(f"Getting history from: {url}")
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read())

def get_videos(ws, prompt):
    prompt_id = queue_prompt(prompt)['prompt_id']
    output_videos = {}
    while True:
        out = ws.recv()
        if isinstance(out, str):
            message = json.loads(out)
            if message['type'] == 'executing':
                data = message['data']
                if data['node'] is None and data['prompt_id'] == prompt_id:
                    break
        else:
            continue

    history = get_history(prompt_id)[prompt_id]
    for node_id in history['outputs']:
        node_output = history['outputs'][node_id]
        videos_output = []
        if 'gifs' in node_output:
            for video in node_output['gifs']:
                # Read file directly using fullpath and encode to base64
                with open(video['fullpath'], 'rb') as f:
                    video_data = base64.b64encode(f.read()).decode('utf-8')
                videos_output.append(video_data)

                # Clean up: delete the video file after processing
                try:
                    os.remove(video['fullpath'])
                    logger.info(f"Cleaned up video file: {video['fullpath']}")
                except OSError as e:
                    logger.warning(f"Failed to delete video file {video['fullpath']}: {e}")
        output_videos[node_id] = videos_output

    return output_videos

def load_workflow(workflow_path):
    with open(workflow_path, 'r') as file:
        return json.load(file)

def handler(job):
    # Log separator at start of each job
    logger.info("=" * 80)
    logger.info("NEW JOB STARTED")
    logger.info("=" * 80)

    job_input = job.get("input", {})

    # Create sanitized copy for logging (avoid logging large base64 strings)
    job_input_log = job_input.copy()
    if "image_base64" in job_input_log and job_input_log["image_base64"]:
        job_input_log["image_base64"] = f"[BASE64_TRUNCATED_{len(job_input_log['image_base64'])}chars]"
    if "end_image_base64" in job_input_log and job_input_log["end_image_base64"]:
        job_input_log["end_image_base64"] = f"[BASE64_TRUNCATED_{len(job_input_log['end_image_base64'])}chars]"

    logger.info(f"Received job input: {job_input_log}")

    # logger.info(f"Received job input: {job_input}")  # Original logging (commented to avoid base64 spam)
    
    task_id = f"task_{uuid.uuid4()}"
    temp_dirs_created = set()  # Track temp directories for cleanup

    # Process image input (use only one of image_path, image_url, image_base64)
    image_path = None
    if "image_path" in job_input:
        image_path = process_input(job_input["image_path"], task_id, "input_image.jpg", "path")
    elif "image_url" in job_input:
        temp_dirs_created.add(task_id)  # Track for cleanup
        image_path = process_input(job_input["image_url"], task_id, "input_image.jpg", "url")
    elif "image_base64" in job_input:
        temp_dirs_created.add(task_id)  # Track for cleanup
        image_path = process_input(job_input["image_base64"], task_id, "input_image.jpg", "base64")
    else:
        # Use default value
        image_path = "/example_image.png"
        logger.info("Using default image file: /example_image.png")

    # Process end image input (use only one of end_image_path, end_image_url, end_image_base64)
    end_image_path_local = None
    if "end_image_path" in job_input:
        end_image_path_local = process_input(job_input["end_image_path"], task_id, "end_image.jpg", "path")
    elif "end_image_url" in job_input:
        temp_dirs_created.add(task_id)  # Track for cleanup
        end_image_path_local = process_input(job_input["end_image_url"], task_id, "end_image.jpg", "url")
    elif "end_image_base64" in job_input:
        temp_dirs_created.add(task_id)  # Track for cleanup
        end_image_path_local = process_input(job_input["end_image_base64"], task_id, "end_image.jpg", "base64")
    
    # Check LoRA configuration - receive as array and process
    lora_pairs = job_input.get("lora_pairs", [])

    # Support up to 4 LoRAs maximum
    lora_count = min(len(lora_pairs), 4)
    if lora_count > len(lora_pairs):
        logger.warning(f"LoRA count is {len(lora_pairs)}. Maximum 4 supported. Using first 4 only.")
        lora_pairs = lora_pairs[:4]

    # Select workflow file (use FLF2V workflow if end_image_* is present)
    workflow_file = "/new_Wan22_flf2v_api.json" if end_image_path_local else "/new_Wan22_api.json"
    logger.info(f"Using {'FLF2V' if end_image_path_local else 'single'} workflow with {lora_count} LoRA pairs")
    
    prompt = load_workflow(workflow_file)
    
    length = job_input.get("length", 81)
    steps = job_input.get("steps", 10)

    prompt["244"]["inputs"]["image"] = image_path
    prompt["541"]["inputs"]["num_frames"] = length
    prompt["135"]["inputs"]["positive_prompt"] = job_input["prompt"]
    prompt["220"]["inputs"]["seed"] = job_input["seed"]
    prompt["540"]["inputs"]["seed"] = job_input["seed"]
    prompt["540"]["inputs"]["cfg"] = job_input["cfg"]
    # Resolution adjustment (width/height to multiples of 16)
    original_width = job_input["width"]
    original_height = job_input["height"]
    adjusted_width = to_nearest_multiple_of_16(original_width)
    adjusted_height = to_nearest_multiple_of_16(original_height)
    if adjusted_width != original_width:
        logger.info(f"Width adjusted to nearest multiple of 16: {original_width} -> {adjusted_width}")
    if adjusted_height != original_height:
        logger.info(f"Height adjusted to nearest multiple of 16: {original_height} -> {adjusted_height}")
    prompt["235"]["inputs"]["value"] = adjusted_width
    prompt["236"]["inputs"]["value"] = adjusted_height
    prompt["498"]["inputs"]["context_overlap"] = job_input.get("context_overlap", 48)
    
    # Apply step settings
    if "834" in prompt:
        prompt["834"]["inputs"]["steps"] = steps
        logger.info(f"Steps set to: {steps}")
        lowsteps = int(steps*0.6)
        prompt["829"]["inputs"]["step"] = lowsteps
        logger.info(f"LowSteps set to: {lowsteps}")

    # If end image exists, apply path to node 617 (FLF2V only)
    if end_image_path_local:
        prompt["617"]["inputs"]["image"] = end_image_path_local

    # Apply LoRA settings - HIGH LoRA is node 279, LOW LoRA is node 553
    if lora_count > 0:
        # HIGH LoRA node (279)
        high_lora_node_id = "279"

        # LOW LoRA node (553)
        low_lora_node_id = "553"

        # Apply received LoRA pairs (starting from lora_1)
        for i, lora_pair in enumerate(lora_pairs):
            if i < 4:  # Maximum 4 only
                lora_high = lora_pair.get("high")
                lora_low = lora_pair.get("low")
                lora_high_weight = lora_pair.get("high_weight", 1.0)
                lora_low_weight = lora_pair.get("low_weight", 1.0)

                # HIGH LoRA settings (node 279, starting from lora_1)
                if lora_high:
                    prompt[high_lora_node_id]["inputs"][f"lora_{i+1}"] = lora_high
                    prompt[high_lora_node_id]["inputs"][f"strength_{i+1}"] = lora_high_weight
                    logger.info(f"LoRA {i+1} HIGH applied to node 279: {lora_high} with weight {lora_high_weight}")
                
                # LOW LoRA settings (node 553, starting from lora_1)
                if lora_low:
                    prompt[low_lora_node_id]["inputs"][f"lora_{i+1}"] = lora_low
                    prompt[low_lora_node_id]["inputs"][f"strength_{i+1}"] = lora_low_weight
                    logger.info(f"LoRA {i+1} LOW applied to node 553: {lora_low} with weight {lora_low_weight}")

    ws_url = f"ws://{server_address}:8188/ws?clientId={client_id}"
    logger.info(f"Connecting to WebSocket: {ws_url}")
    
    # First check if HTTP connection is possible
    http_url = f"http://{server_address}:8188/"
    logger.info(f"Checking HTTP connection to: {http_url}")

    # HTTP connection check (maximum 1 minute)
    max_http_attempts = 180
    for http_attempt in range(max_http_attempts):
        try:
            import urllib.request
            response = urllib.request.urlopen(http_url, timeout=5)
            logger.info(f"HTTP connection successful (attempt {http_attempt+1})")
            break
        except Exception as e:
            logger.warning(f"HTTP connection failed (attempt {http_attempt+1}/{max_http_attempts}): {e}")
            if http_attempt == max_http_attempts - 1:
                raise Exception("Cannot connect to ComfyUI server. Make sure the server is running.")
            time.sleep(1)

    ws = websocket.WebSocket()
    # WebSocket connection attempt (maximum 3 minutes)
    max_attempts = int(180/5)  # 3 minutes (attempt every 1 second)
    for attempt in range(max_attempts):
        import time
        try:
            ws.connect(ws_url)
            logger.info(f"WebSocket connection successful (attempt {attempt+1})")
            break
        except Exception as e:
            logger.warning(f"WebSocket connection failed (attempt {attempt+1}/{max_attempts}): {e}")
            if attempt == max_attempts - 1:
                raise Exception("WebSocket connection timeout (3 minutes)")
            time.sleep(5)
    videos = get_videos(ws, prompt)
    ws.close()

    # Log separator between jobs for better log readability
    logger.info("=" * 80)
    logger.info("JOB PROCESSING COMPLETE - STARTING CLEANUP")
    logger.info("=" * 80)

    # Clean up temporary directories after processing
    for temp_dir in temp_dirs_created:
        try:
            shutil.rmtree(temp_dir)
            logger.info(f"Cleaned up temporary directory: {temp_dir}")
        except Exception as e:
            logger.warning(f"Failed to clean up temp directory {temp_dir}: {e}")

    # Clean up ComfyUI output directory (removes PNGs, intermediate files, etc.)
    output_dir = "/ComfyUI/output"
    try:
        if os.path.exists(output_dir):
            # Remove all files and subdirectories in output directory
            for filename in os.listdir(output_dir):
                file_path = os.path.join(output_dir, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                        logger.info(f"Removed file: {file_path}")
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                        logger.info(f"Removed directory: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to remove {file_path}: {e}")
        else:
            logger.info("ComfyUI output directory does not exist")
    except Exception as e:
        logger.warning(f"Failed to clean ComfyUI output directory: {e}")

    # Handle case where no images are found
    for node_id in videos:
        if videos[node_id]:
            return {"video": videos[node_id][0]}

    return {"error": "Video not found."}

runpod.serverless.start({"handler": handler})
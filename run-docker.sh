#!/bin/bash

# Snowflake Streaming Ingest Example - Docker Runner Script
# This script helps you build and run the Docker image

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
IMAGE_NAME="snowflake-streaming-ingest"
CONTAINER_NAME="snowflake-streaming-ingest"
PROFILE_FILE="profile.json"

print_usage() {
    echo "Usage: $0 [build|buildx|run|run-embedded|run-with-tag|clean|help]"
    echo ""
    echo "Commands:"
    echo "  build        - Build the Docker image"
    echo "  buildx       - Build with buildx for specific platform"
    echo "                 Usage: $0 buildx [platform] [tag_suffix]"
    echo "                 Example: $0 buildx linux/amd64 amd64"
    echo "  run          - Run the Docker container with mounted config"
    echo "  run-embedded - Run the Docker container with embedded config (no volume mount)"
    echo "  run-with-tag - Run container with custom image tag"
    echo "                 Usage: $0 run-with-tag [image_tag] [container_suffix]"
    echo "                 Example: $0 run-with-tag snowflake-streaming-ingest:amd64 -amd64"
    echo "  clean        - Clean up Docker resources"
    echo "  help         - Show this help message"
    echo ""
    echo "Before running, ensure you have a valid profile.json file in the current directory."
    echo "For embedded config, ensure profile.json is copied into the image during build."
}

check_profile() {
    if [ ! -f "$PROFILE_FILE" ]; then
        echo -e "${RED}Error: $PROFILE_FILE not found!${NC}"
        echo "Please create a $PROFILE_FILE file with your Snowflake connection details."
        echo "See profile_streaming.json.example for reference."
        return 1
    fi
    echo -e "${GREEN}✓ Found $PROFILE_FILE${NC}"
}

build_image() {
    echo -e "${YELLOW}Building Docker image: $IMAGE_NAME${NC}"
    docker build -t "$IMAGE_NAME" .
    echo -e "${GREEN}✓ Docker image built successfully${NC}"
}

# New function for buildx builds with platform specification
build_image_buildx() {
    local platform=${1:-"linux/amd64"}
    local tag_suffix=${2:-"amd64"}
    local image_tag="$IMAGE_NAME:$tag_suffix"
    
    echo -e "${YELLOW}Building Docker image with buildx: $image_tag (platform: $platform)${NC}"
    
    # Check if buildx is available
    if ! docker buildx version &>/dev/null; then
        echo -e "${RED}Error: docker buildx is not available${NC}"
        echo "Please install Docker with buildx support or use 'docker build' instead."
        return 1
    fi
    
    # Create builder if it doesn't exist
    if ! docker buildx ls | grep -q "multiarch"; then
        echo "Creating buildx builder..."
        docker buildx create --name multiarch --use
    fi
    
    # Build with buildx
    docker buildx build --platform "$platform" -t "$image_tag" . --load
    
    echo -e "${GREEN}✓ Docker image built successfully with buildx${NC}"
    echo -e "${GREEN}✓ Image tagged as: $image_tag${NC}"
}

run_container() {
    echo -e "${YELLOW}Checking prerequisites...${NC}"
    check_profile || return 1
    
    echo -e "${YELLOW}Running Docker container: $CONTAINER_NAME${NC}"
    
    # Remove existing container if it exists
    if docker ps -a --format 'table {{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
        echo "Removing existing container..."
        docker rm -f "$CONTAINER_NAME"
    fi
    
    # Run the container
    docker run --name "$CONTAINER_NAME" \
        -v "$(pwd)/$PROFILE_FILE:/app/config/profile.json:ro" \
        "$IMAGE_NAME"
    
    echo -e "${GREEN}✓ Container execution completed${NC}"
}

# Alternative function for embedded config (no volume mount needed)
run_container_embedded() {
    echo -e "${YELLOW}Running Docker container with embedded config: $CONTAINER_NAME${NC}"
    
    # Remove existing container if it exists
    if docker ps -a --format 'table {{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
        echo "Removing existing container..."
        docker rm -f "$CONTAINER_NAME"
    fi
    
    # Run the container (no volume mount needed)
    docker run --name "$CONTAINER_NAME" "$IMAGE_NAME"
    
    echo -e "${GREEN}✓ Container execution completed${NC}"
}

# Function to run container with custom image tag (for buildx images)
run_container_with_tag() {
    local image_tag=${1:-"$IMAGE_NAME"}
    local container_name_suffix=${2:-""}
    local full_container_name="$CONTAINER_NAME$container_name_suffix"
    
    echo -e "${YELLOW}Running Docker container with image: $image_tag${NC}"
    
    # Remove existing container if it exists
    if docker ps -a --format 'table {{.Names}}' | grep -q "^$full_container_name$"; then
        echo "Removing existing container..."
        docker rm -f "$full_container_name"
    fi
    
    # Run the container (no volume mount needed since config is embedded)
    docker run --name "$full_container_name" "$image_tag"
    
    echo -e "${GREEN}✓ Container execution completed${NC}"
}

clean_resources() {
    echo -e "${YELLOW}Cleaning up Docker resources...${NC}"
    
    # Remove container
    if docker ps -a --format 'table {{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
        echo "Removing container: $CONTAINER_NAME"
        docker rm -f "$CONTAINER_NAME"
    fi
    
    # Remove image
    if docker images --format 'table {{.Repository}}' | grep -q "^$IMAGE_NAME$"; then
        echo "Removing image: $IMAGE_NAME"
        docker rmi "$IMAGE_NAME"
    fi
    
    echo -e "${GREEN}✓ Cleanup completed${NC}"
}

# Main script logic
case "$1" in
    build)
        build_image
        ;;
    buildx)
        if [ -z "$2" ]; then
            echo -e "${RED}Error: Platform argument is required for 'buildx' command.${NC}"
            echo "Usage: $0 buildx [platform] [tag_suffix]"
            echo "Example: $0 buildx linux/amd64 amd64"
            exit 1
        fi
        build_image_buildx "$2" "${3:-"amd64"}"
        ;;
    run)
        run_container
        ;;
    run-embedded)
        run_container_embedded
        ;;
    run-with-tag)
        if [ -z "$2" ]; then
            echo -e "${RED}Error: Image tag argument is required for 'run-with-tag' command.${NC}"
            echo "Usage: $0 run-with-tag [image_tag] [container_suffix]"
            echo "Example: $0 run-with-tag snowflake-streaming-ingest:amd64 -amd64"
            exit 1
        fi
        run_container_with_tag "$2" "${3:-""}"
        ;;
    clean)
        clean_resources
        ;;
    help|--help|-h)
        print_usage
        ;;
    "")
        echo -e "${YELLOW}No command specified. Building and running...${NC}"
        build_image
        run_container
        ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        print_usage
        exit 1
        ;;
esac

echo -e "${GREEN}Done!${NC}" 
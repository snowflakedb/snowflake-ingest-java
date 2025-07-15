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
    echo "Usage: $0 [build|run|clean|help]"
    echo ""
    echo "Commands:"
    echo "  build   - Build the Docker image"
    echo "  run     - Run the Docker container"
    echo "  clean   - Clean up Docker resources"
    echo "  help    - Show this help message"
    echo ""
    echo "Before running, ensure you have a valid profile.json file in the current directory."
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
    run)
        run_container
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
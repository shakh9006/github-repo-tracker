#!/bin/bash
# GitHub Repo Tracker Orchestrator Script
# 
# This script orchestrates the startup and shutdown of a complete GitHub Repo Tracker stack:
# - Airflow (Workflow orchestrator)
# Usage: ./manage-github-repo-tracker.sh [start|stop|stop-and-clean-up]

set -e  # Exit immediately if any command fails

# Get the absolute path of the script directory to ensure relative paths work correctly
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Function to start all services in the correct order
start_services() {
    echo "Starting services..."
    
    # Change to script directory to ensure docker-compose files are found
    cd "$SCRIPT_DIR"

    # Step *: Create Network if it doesn't exist
    if ! docker network ls | grep -q github-repo-tracker; then
        echo "Creating network github-repo-tracker..."
        docker network create github-repo-tracker
    fi
    

    # Step 1: Start the airflow services (Airflow)
    echo "Starting airflow services (Airflow)..."
    docker compose -f ./airflow/docker-compose.yaml up -d --build
    sleep 5  # Allow services to initialize

    echo "All services started successfully."
    echo ""
    echo "Service Access Information:"
    echo "  - Airflow: http://localhost:8080"
    echo ""
}

# Function to stop all services and clean up resources
stop_and_clean_up_services() {
    echo "Stopping and cleaning up services..."
    
    # Change to script directory
    cd "$SCRIPT_DIR"
    
    # Stop services in reverse order (Airflow)
    echo "Stopping airflow services..."
    docker compose -f ./airflow/docker-compose.yaml down -v

    echo "All services stopped and volumes cleaned up."
    echo ""
}

# Function to stop all services and clean up resources
stop_services() {
    echo "Stopping services..."
    
    # Change to script directory
    cd "$SCRIPT_DIR"
    
    # Stop services in reverse order (Airflow)
    echo "Stopping airflow services..."
    docker compose -f ./airflow/docker-compose.yaml down

    echo "All services stopped."
    echo ""
}

# Main script logic - handle command line arguments
case "${1:-help}" in
    "start")
        start_services
        ;;
    "stop")
        stop_services
        ;;
    "stop-and-clean-up")
        stop_and_clean_up_services
        ;;
    *)
        echo "GitHub Repo Tracker Management Script"
        echo ""
        echo "Usage: $0 [start|stop|stop-and-clean-up]"
        echo ""
        echo "Commands:"
        echo "  start    Start all services (Airflow)"
        echo "  stop     Stop all services"
        echo "  stop-and-clean-up     Stop all services and clean up volumes"
        echo ""
        echo "Examples:"
        echo "  $0 start    # Start all services"
        echo "  $0 stop     # Stop all services"
        echo "  $0 stop-and-clean-up     # Stop all services and clean up volumes"
        echo ""
        echo "After starting, you can access:"
        echo "  - Airflow: http://localhost:8080"
        ;;
esac
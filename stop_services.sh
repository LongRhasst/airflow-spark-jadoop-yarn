echo "All services are being stopped..."
docker compose down
echo "Cleaning up containers and networks..."
docker system prune -f
echo "All services have been stopped and cleaned up."
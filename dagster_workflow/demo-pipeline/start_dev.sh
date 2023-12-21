# Assumes a container with image from https://min.io/docs/minio/container/index.html already exists named "minio"

container_names=$(docker ps -a --format '{{.Names}}' --filter "ancestor=quay.io/minio/minio")

if [ -n "$container_names" ]; then
    echo "Containers with image quay.io/minio/minio found:"
    echo "$container_names"
    first_container_name=$(echo "$container_names" | head -n 1)
    echo "will start $first_container_name, if this is incorrect run 'docker stop $first_container_name' and start the desired container"
    echo "more information about the container can be found with docker ps"
    docker restart $first_container_name
else
    echo "No containers with image quay.io/minio/minio found."
    echo "Running default dev set up with user=user and password=password and ports 9090 and 9000 bound."
    echo "Data storage at ${HOME}/minio/data"

    mkdir -p ${HOME}/minio/data

    docker run \
       -p 9000:9000 \
       -p 9090:9090 \
       --user $(id -u):$(id -g) \
       --name minio \
       -e "MINIO_ROOT_USER=user" \
       -e "MINIO_ROOT_PASSWORD=password" \
       -v ${HOME}/minio/data:/data \
       quay.io/minio/minio server /data --console-address ":9090"
fi

dagster dev

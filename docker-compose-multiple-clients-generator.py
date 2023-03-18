from sys import argv

DOCKER_COMPOSE_NAME = "docker-compose-multiple-clients.yaml"
ORIGINAL_DOCKER_COMPOSE_NAME = "docker-compose-dev.yaml"
NUMBER_OF_CLIENTS = 1

def copyOriginalDockerCompose(new_docker_compose_file):
    with open(new_docker_compose_file, "w") as docker_compose:
        with open(ORIGINAL_DOCKER_COMPOSE_NAME, "r") as original_docker_compose:
            for line in original_docker_compose:
                docker_compose.write(line)
    docker_compose.close()
    original_docker_compose.close()

def addClients(number_of_clients, docker_compose_name):
    docker_compose = open(docker_compose_name, "a")
    for i in range(2, number_of_clients + 1):
        docker_compose.write(f"  client{i}:\n")
        docker_compose.write(f"      <<: *client\n")
        docker_compose.write(f"      container_name: client{i}\n")
        docker_compose.write(f"      environment:\n")
        docker_compose.write(f"          - CLI_ID={i}\n")
        docker_compose.write(f"          - CLI_LOG_LEVEL=DEBUG\n")
    docker_compose.close()

def generateDockerCompose(number_of_clients, docker_compose_name):
    copyOriginalDockerCompose(docker_compose_name)
    addClients(number_of_clients, docker_compose_name)


def main():
    number_of_clients = NUMBER_OF_CLIENTS
    docker_compose_name = DOCKER_COMPOSE_NAME
    
    if len(argv) > 1:
        try:
            number_of_clients = int(argv[1])
        except ValueError:
            print("Number of clients must be an integer")
            exit(-1)
    if (len(argv) > 2):
        docker_compose_name = argv[2]
    
    if (docker_compose_name == ORIGINAL_DOCKER_COMPOSE_NAME):
        print("Error: The name of the new docker compose file must be different from the original docker compose file")
        exit(-1)

    try:
        generateDockerCompose(number_of_clients, docker_compose_name)
        print(f"Docker compose file {docker_compose_name} generated successfully with {number_of_clients} clients")
    except Exception as e:
        print(f"Error: {e}")
        exit(-1)

if __name__ == "__main__":
    main()

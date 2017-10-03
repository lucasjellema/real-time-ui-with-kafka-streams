Here the link to the Docker Compose for Kafka:

https://gist.github.com/gschmutz/db582679c07c11f645b8cb9718e31209

It includes:

-          1x Zookeeper
-          1x Kafka Broker
-          1x Kafka Connect: http://localhost:8083
-          1x Schema Registry            
-          1x Landoop Schema-Registry UI: http://localhost:8002/
-          1x Landoop Connect UI: http://localhost:8001
-          1x KafkaManager: http://localhost:9000

To start it, just do (first in docker-compose.yml replace the IP address of your Docker Host i.e. the Linux VM running the Docker Container):
replace all occurrences of 192.168.188.102 with the IP address assigned to the Docker Host (when using Vagrant, then this is the IP address specified in the Vagrantfile under config.vm.network "private_network)



export DOCKER_HOST_IP=192.168.188.102

docker-compose up -d

docker-compose logs -f


I have used https://gist.github.com/softinio/7e34eaaa816fd65f3d5cabfa5cc0b8ec

to first install vagrant plugin for docker compose
then to adapt vagrant file
then to run vagrant


vagrant ssh

cd /vagrant
docker-compose logs -f

check what is happening inside the DOcker Containers: Kafka is started

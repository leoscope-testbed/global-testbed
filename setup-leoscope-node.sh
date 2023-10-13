mkdir leoscope
cd leoscope

# turn off NIC offloading for two reasons: 
# 1. Segments captured by tshark/tcpdump should be same length as those that actually go on to the wire (by avodiing fragmentation in the NIC) 
# 2. UDP packets of size below 12 bytes are dropped automatically by NIC on some cloud providers (ex, Azure) which might impact the experiments. 

ethtool --offload  eth0  rx off  tx off
ethtool -K eth0 gso off

sudo update-alternatives --config iptables

sudo echo '{
  "dns": ["8.8.8.8", "192.168.0.1"]
}' > /etc/docker/daemon.json

# choose legacy option manual mode

for pkg in docker.io docker-doc docker-compose podman-docker containerd runc; do sudo apt-get remove $pkg; done

sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update

sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# docker installation done.

sudo curl -L "https://github.com/docker/compose/releases/download/v2.12.2/docker-compose-linux-x86_64" -o /usr/local/bin/docker-compose

sudo chmod +x /usr/local/bin/docker-compose

docker-compose --version

# docker-compose installation done.

git clone https://github.com/leopard-testbed/global-testbed.git

cd global-testbed

git fetch

git branch -v -a

git switch dev

cd ../
mkdir jobdir
cd jobdir
pwd

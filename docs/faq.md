# Frequently Asked Questions

1. I can't login to docker after ./setup.sh?
* Logout and login to the machine you are on and use ```docker login```. <br>
* The setup script adds the user to the docker group. If the user is not already configured to the group, they will need to restart the machine for this change to be updated.  
<br>

2. Do I need to do the ./setup.sh step if I already have docker installed?
* If you have a docker verison that is greater than  20.10.0, then you do not need to run ./setup.sh

<br>

3. How do I get the proxy ip and port for the junoload command?
* Find the proxy ip by entering ``` hostname -i``` command on the proxy machine<br>
* Go to junodb/package_config/package/junoserv/config/config.toml<br>
* Find the proxy listener port under ListenerPort (Example: 5080)<br>
* Note: Use the TLS port with SSL enabled when using the -ssl flag

<br>

4. How do I open the proxy monitoring page?<br>
* Go to junodb/package_config/package/junoserv/config/config.toml<br>
* Find the proxy monitoring port under parameter HttpMonAddr (Example: 8088)<br>
* Find the proxy ip by entering ``` hostname -i``` command on the proxy machine<br>
* In a web browser, in the URL box, type <proxy_ip>:<proxy_monitoring_port><br>
* This should open the proxy monitoring page<br>



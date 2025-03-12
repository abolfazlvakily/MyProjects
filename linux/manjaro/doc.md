# Mirrors

### Update and set mirrors.

```shell
sudo pacman-mirrors --fasttrack 5
```

### Set custom mirror
```shell
sudo nano /etc/pacman.d/mirrorlist
```
for example:
```shell
## Germany
Server = https://mirror.alpix.eu/manjaro/stable/$repo/$arch
```


### Update
```shell
sudo pacman -Syyu
```
explain params:
```shell
S -> for upadte or install your pack.
y -> does the database need an update?
yy -> force refresh repos.
u -> upgrade or update all packs.
```

### Show fast mirrors
```shell
sudo pacman-mirrors --status
```

# Openvpn

```shell
sudo pacman -S openvpn
```
show version
```shell
openvpn --version
```

run
````shell
mv fr.ovpn fr.conf
sudo openvpn --confing fr.conf
````

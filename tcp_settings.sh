sysctl net.ipv4.ip_local_port_range="15000 61000"
sysctl net.ipv4.tcp_fin_timeout=30

sysctl net.ipv4.tcp_tw_recycle=1
sysctl net.ipv4.tcp_tw_reuse=1 

ifconfig enp126s0 txqueuelen 5000
echo "/sbin/ifconfig enp126s0 txqueuelen 5000" >> /etc/rc.local

sysctl net.core.netdev_max_backlog=2000

ulimit -n 500000

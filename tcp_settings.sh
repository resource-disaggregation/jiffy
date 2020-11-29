sysctl net.ipv4.ip_local_port_range="15000 61000"
sysctl net.ipv4.tcp_fin_timeout=30


ifconfig enp126s0 txqueuelen 5000
echo "/sbin/ifconfig enp126s0 txqueuelen 5000" >> /etc/rc.local


sysctl net.core.somaxconn=8096
sysctl net.core.netdev_max_backlog=5000
sysctl net.core.rmem_max=16777216
sysctl net.core.wmem_max=16777216
sysctl net.ipv4.tcp_wmem="4096 12582912 16777216"
sysctl net.ipv4.tcp_rmem="4096 12582912 16777216"
sysctl net.ipv4.tcp_max_syn_backlog=8096
sysctl net.ipv4.tcp_slow_start_after_idle=0
sysctl net.ipv4.tcp_tw_reuse=1

ulimit -n 500000
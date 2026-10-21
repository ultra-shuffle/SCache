The original experimental source linked scache.conf and slaves to absolute
paths on the development machine. Those links are intentionally omitted from
this portable source archive. The two-host RM/CTP runner creates per-node
scache.conf files in its own workspace. For manual runs, create local config
files for the target hosts; do not reuse development-machine addresses.

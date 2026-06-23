#!/bin/bash
# Runs in each container just before the HTCondor daemons start (the base image
# invokes /root/config/pre-exec.sh from start.sh).
#
# Make the shared /staging mount writable by both the AP's submituser and the
# EP's per-slot job accounts (slot1_N). 1777 = world-writable with the sticky
# bit, the same mode used for /tmp.
mkdir -p /staging
chmod 1777 /staging

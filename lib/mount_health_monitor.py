#!/usr/bin/env python3
"""
Kadalu Mount Health Monitor
Continuously monitors GlusterFS mounts for I/O errors and automatically recovers from issues.
This script helps prevent I/O errors during network flaps and slow disk scenarios.
"""

import os
import sys
import time
import logging
import subprocess
import threading
import signal
from pathlib import Path
from typing import Dict, List, Set
from collections import defaultdict

# Add kadalu lib to path
sys.path.insert(0, '/kadalu')
try:
    from kadalulib import logf, is_gluster_mount_proc_running
    from volumeutils import verify_mount, mount_glusterfs, unmount_glusterfs
except ImportError:
    # Fallback for testing outside container
    def logf(msg, **kwargs):
        return f"{msg}: {kwargs}"

class MountHealthMonitor:
    """Monitor and maintain health of GlusterFS mounts"""
    
    def __init__(self, check_interval: int = 30, recovery_attempts: int = 3):
        self.check_interval = check_interval
        self.recovery_attempts = recovery_attempts
        self.monitored_mounts: Dict[str, Dict] = {}
        self.lock = threading.Lock()
        self.running = True
        self.consecutive_failures: Dict[str, int] = defaultdict(int)
        self.last_recovery_attempt: Dict[str, float] = {}
        self.min_recovery_interval = 300  # 5 minutes between recovery attempts
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('/var/log/gluster/mount-health.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def add_mount(self, mountpoint: str, volume_info: Dict):
        """Add a mount to monitor"""
        with self.lock:
            self.monitored_mounts[mountpoint] = {
                'volume_info': volume_info,
                'last_check': 0,
                'health_status': 'unknown',
                'error_count': 0,
                'last_error': None,
                'recovery_count': 0
            }
            self.logger.info(logf(
                "Added mount to health monitor",
                mountpoint=mountpoint,
                volume_name=volume_info.get('name', 'unknown')
            ))
    
    def remove_mount(self, mountpoint: str):
        """Remove a mount from monitoring"""
        with self.lock:
            if mountpoint in self.monitored_mounts:
                del self.monitored_mounts[mountpoint]
                self.consecutive_failures.pop(mountpoint, None)
                self.last_recovery_attempt.pop(mountpoint, None)
                self.logger.info(logf(
                    "Removed mount from health monitor",
                    mountpoint=mountpoint
                ))
    
    def check_mount_health(self, mountpoint: str) -> bool:
        """Check health of a specific mount"""
        try:
            # Quick verification with shorter timeout
            mount_healthy = verify_mount(mountpoint, timeout=15)
            
            if mount_healthy:
                self.consecutive_failures[mountpoint] = 0
                return True
            else:
                self.consecutive_failures[mountpoint] += 1
                self.logger.warning(logf(
                    "Mount health check failed",
                    mountpoint=mountpoint,
                    consecutive_failures=self.consecutive_failures[mountpoint]
                ))
                return False
                
        except Exception as e:
            self.consecutive_failures[mountpoint] += 1
            self.logger.error(logf(
                "Mount health check exception",
                mountpoint=mountpoint,
                error=str(e),
                consecutive_failures=self.consecutive_failures[mountpoint]
            ))
            return False
    
    def detect_io_errors(self, mountpoint: str) -> List[str]:
        """Detect I/O errors in mount logs and dmesg"""
        io_errors = []
        
        try:
            # Check dmesg for filesystem errors
            dmesg_output = subprocess.check_output(
                ['dmesg', '-T'], 
                timeout=10,
                stderr=subprocess.DEVNULL
            ).decode()
            
            error_patterns = [
                f"Transport endpoint is not connected.*{mountpoint}",
                f"I/O error.*{mountpoint}",
                f"Stale file handle.*{mountpoint}",
                f"Connection reset.*{mountpoint}",
                f"Remote I/O error.*{mountpoint}"
            ]
            
            for pattern in error_patterns:
                if pattern.lower() in dmesg_output.lower():
                    io_errors.append(f"dmesg: {pattern}")
            
            # Check GlusterFS logs
            gluster_log = "/var/log/gluster/gluster.log"
            if os.path.exists(gluster_log):
                try:
                    # Get last 100 lines to avoid reading huge logs
                    log_output = subprocess.check_output(
                        ['tail', '-100', gluster_log],
                        timeout=5
                    ).decode()
                    
                    log_error_patterns = [
                        "Transport endpoint is not connected",
                        "Connection failed",
                        "rpc_clnt_ping_timer_expired",
                        "socket disconnected",
                        "Stale file handle"
                    ]
                    
                    for pattern in log_error_patterns:
                        if pattern.lower() in log_output.lower():
                            io_errors.append(f"gluster.log: {pattern}")
                            
                except subprocess.TimeoutExpired:
                    self.logger.warning("Timeout reading GlusterFS log")
                    
        except Exception as e:
            self.logger.warning(logf(
                "Error checking I/O error logs",
                mountpoint=mountpoint,
                error=str(e)
            ))
        
        return io_errors
    
    def attempt_recovery(self, mountpoint: str, volume_info: Dict) -> bool:
        """Attempt to recover a failed mount"""
        current_time = time.time()
        last_attempt = self.last_recovery_attempt.get(mountpoint, 0)
        
        # Rate limit recovery attempts
        if current_time - last_attempt < self.min_recovery_interval:
            remaining_time = self.min_recovery_interval - (current_time - last_attempt)
            self.logger.info(logf(
                "Recovery attempt rate limited",
                mountpoint=mountpoint,
                remaining_cooldown_seconds=int(remaining_time)
            ))
            return False
        
        self.last_recovery_attempt[mountpoint] = current_time
        
        try:
            self.logger.info(logf(
                "Attempting mount recovery",
                mountpoint=mountpoint,
                volume_name=volume_info.get('name', 'unknown')
            ))
            
            # Step 1: Try to unmount gracefully
            try:
                unmount_glusterfs(mountpoint)
                time.sleep(5)  # Wait for cleanup
            except Exception as e:
                self.logger.warning(logf(
                    "Graceful unmount failed during recovery",
                    mountpoint=mountpoint,
                    error=str(e)
                ))
            
            # Step 2: Force cleanup if still mounted
            try:
                if os.path.ismount(mountpoint):
                    subprocess.run(['umount', '-f', '-l', mountpoint], 
                                 timeout=30, check=False)
                    time.sleep(3)
            except Exception as e:
                self.logger.warning(logf(
                    "Force unmount failed during recovery",
                    mountpoint=mountpoint,
                    error=str(e)
                ))
            
            # Step 3: Clear any stale processes
            try:
                volname = volume_info.get('name', os.path.basename(mountpoint))
                subprocess.run(['pkill', '-f', f'glusterfs.*{volname}'], 
                             check=False, timeout=10)
                time.sleep(2)
            except Exception as e:
                self.logger.debug(logf(
                    "Process cleanup during recovery",
                    mountpoint=mountpoint,
                    error=str(e)
                ))
            
            # Step 4: Attempt remount
            try:
                mount_glusterfs(volume_info, mountpoint)
                time.sleep(5)  # Allow mount to stabilize
                
                # Verify recovery
                if verify_mount(mountpoint, timeout=30):
                    self.logger.info(logf(
                        "Mount recovery successful",
                        mountpoint=mountpoint,
                        volume_name=volume_info.get('name', 'unknown')
                    ))
                    
                    with self.lock:
                        if mountpoint in self.monitored_mounts:
                            self.monitored_mounts[mountpoint]['recovery_count'] += 1
                            self.monitored_mounts[mountpoint]['health_status'] = 'healthy'
                            self.monitored_mounts[mountpoint]['error_count'] = 0
                    
                    return True
                else:
                    self.logger.error(logf(
                        "Mount recovery failed - verification failed",
                        mountpoint=mountpoint
                    ))
                    return False
                    
            except Exception as e:
                self.logger.error(logf(
                    "Mount recovery failed - remount failed",
                    mountpoint=mountpoint,
                    error=str(e)
                ))
                return False
                
        except Exception as e:
            self.logger.error(logf(
                "Mount recovery failed with exception",
                mountpoint=mountpoint,
                error=str(e)
            ))
            return False
    
    def monitor_loop(self):
        """Main monitoring loop"""
        self.logger.info("Mount health monitor started")
        
        while self.running:
            try:
                current_time = time.time()
                
                with self.lock:
                    mounts_to_check = list(self.monitored_mounts.items())
                
                for mountpoint, mount_info in mounts_to_check:
                    try:
                        # Check if it's time to check this mount
                        if current_time - mount_info['last_check'] < self.check_interval:
                            continue
                        
                        # Update check time
                        with self.lock:
                            if mountpoint in self.monitored_mounts:
                                self.monitored_mounts[mountpoint]['last_check'] = current_time
                        
                        # Perform health check
                        is_healthy = self.check_mount_health(mountpoint)
                        
                        if is_healthy:
                            with self.lock:
                                if mountpoint in self.monitored_mounts:
                                    self.monitored_mounts[mountpoint]['health_status'] = 'healthy'
                                    self.monitored_mounts[mountpoint]['error_count'] = 0
                        else:
                            # Mount is unhealthy
                            with self.lock:
                                if mountpoint in self.monitored_mounts:
                                    self.monitored_mounts[mountpoint]['health_status'] = 'unhealthy'
                                    self.monitored_mounts[mountpoint]['error_count'] += 1
                            
                            # Check for I/O errors
                            io_errors = self.detect_io_errors(mountpoint)
                            if io_errors:
                                self.logger.warning(logf(
                                    "I/O errors detected",
                                    mountpoint=mountpoint,
                                    errors=io_errors
                                ))
                            
                            # Attempt recovery if enough failures
                            failure_count = self.consecutive_failures[mountpoint]
                            if failure_count >= 3:  # Recover after 3 consecutive failures
                                volume_info = mount_info['volume_info']
                                recovery_success = self.attempt_recovery(mountpoint, volume_info)
                                
                                if recovery_success:
                                    self.consecutive_failures[mountpoint] = 0
                                else:
                                    self.logger.error(logf(
                                        "Mount recovery failed, will retry later",
                                        mountpoint=mountpoint,
                                        failure_count=failure_count
                                    ))
                    
                    except Exception as e:
                        self.logger.error(logf(
                            "Error checking mount",
                            mountpoint=mountpoint,
                            error=str(e)
                        ))
                
                # Sleep until next check
                time.sleep(min(self.check_interval, 10))
                
            except Exception as e:
                self.logger.error(logf(
                    "Error in monitor loop",
                    error=str(e)
                ))
                time.sleep(10)
    
    def get_status(self) -> Dict:
        """Get current monitoring status"""
        with self.lock:
            return {
                'monitored_mounts': len(self.monitored_mounts),
                'mounts': {
                    mp: {
                        'health_status': info['health_status'],
                        'error_count': info['error_count'],
                        'recovery_count': info['recovery_count'],
                        'consecutive_failures': self.consecutive_failures.get(mp, 0)
                    }
                    for mp, info in self.monitored_mounts.items()
                }
            }
    
    def stop(self):
        """Stop the monitor"""
        self.running = False
        self.logger.info("Mount health monitor stopped")


# Global monitor instance
mount_monitor = None

def start_mount_monitor():
    """Start the global mount monitor"""
    global mount_monitor
    if mount_monitor is None:
        mount_monitor = MountHealthMonitor()
        monitor_thread = threading.Thread(target=mount_monitor.monitor_loop, daemon=True)
        monitor_thread.start()
        
def add_mount_to_monitor(mountpoint: str, volume_info: Dict):
    """Add a mount to the global monitor"""
    global mount_monitor
    if mount_monitor:
        mount_monitor.add_mount(mountpoint, volume_info)

def remove_mount_from_monitor(mountpoint: str):
    """Remove a mount from the global monitor"""
    global mount_monitor
    if mount_monitor:
        mount_monitor.remove_mount(mountpoint)

def get_monitor_status() -> Dict:
    """Get status of the global monitor"""
    global mount_monitor
    if mount_monitor:
        return mount_monitor.get_status()
    return {'error': 'Monitor not running'}


if __name__ == "__main__":
    # Standalone monitor for testing
    def signal_handler(signum, frame):
        if mount_monitor:
            mount_monitor.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    monitor = MountHealthMonitor()
    
    # Add test mounts from command line
    if len(sys.argv) > 1:
        for mountpoint in sys.argv[1:]:
            if os.path.isdir(mountpoint):
                volume_info = {
                    'name': os.path.basename(mountpoint),
                    'type': 'test'
                }
                monitor.add_mount(mountpoint, volume_info)
    
    try:
        monitor.monitor_loop()
    except KeyboardInterrupt:
        monitor.stop()

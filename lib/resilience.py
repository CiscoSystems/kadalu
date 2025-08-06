"""
Enhanced resilience patterns for Kadalu CSI operations
Provides circuit breaker, exponential backoff, and health checking utilities
"""

import time
import threading
import logging
from enum import Enum
from typing import Dict, Callable, Any, Optional
from collections import defaultdict

from kadalulib import logf


class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, rejecting requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """
    Circuit breaker pattern implementation for Kadalu operations
    """
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60, 
                 success_threshold: int = 3, name: str = "default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self.name = name
        
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
        self.lock = threading.Lock()
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection"""
        
        with self.lock:
            if self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    logging.info(logf(
                        "Circuit breaker transitioning to half-open",
                        circuit_name=self.name,
                        failure_count=self.failure_count
                    ))
                else:
                    raise Exception(f"Circuit breaker {self.name} is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e
    
    def _on_success(self):
        """Handle successful operation"""
        with self.lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    logging.info(logf(
                        "Circuit breaker closed after successful recovery",
                        circuit_name=self.name,
                        success_count=self.success_count
                    ))
            elif self.state == CircuitState.CLOSED:
                self.failure_count = max(0, self.failure_count - 1)
    
    def _on_failure(self):
        """Handle failed operation"""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.state == CircuitState.CLOSED and self.failure_count >= self.failure_threshold:
                self.state = CircuitState.OPEN
                logging.warning(logf(
                    "Circuit breaker opened due to failures",
                    circuit_name=self.name,
                    failure_count=self.failure_count,
                    threshold=self.failure_threshold
                ))
            elif self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                logging.warning(logf(
                    "Circuit breaker reopened during half-open state",
                    circuit_name=self.name
                ))


class ExponentialBackoff:
    """
    Exponential backoff with jitter for retry operations
    """
    
    def __init__(self, base_delay: float = 1.0, max_delay: float = 60.0, 
                 backoff_factor: float = 2.0, jitter: bool = True):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter
    
    def get_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt number (0-based)"""
        delay = self.base_delay * (self.backoff_factor ** attempt)
        delay = min(delay, self.max_delay)
        
        if self.jitter:
            # Add jitter: ±10% of the delay
            jitter_amount = delay * 0.1
            jitter_offset = (time.time() % 1 - 0.5) * 2 * jitter_amount
            delay += jitter_offset
        
        return max(0, delay)


class HealthChecker:
    """
    Health checking utility for Kadalu services
    """
    
    def __init__(self, check_interval: int = 30):
        self.check_interval = check_interval
        self.health_status: Dict[str, Dict] = defaultdict(dict)
        self.lock = threading.Lock()
    
    def check_host_health(self, host: str, port: int, timeout: int = 10) -> bool:
        """Check if a host is healthy"""
        import socket
        
        try:
            if ':' in host:  # IPv6
                sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            else:  # IPv4
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
            sock.settimeout(timeout)
            sock.connect((host, port))
            sock.close()
            
            with self.lock:
                self.health_status[host] = {
                    'healthy': True,
                    'last_check': time.time(),
                    'consecutive_failures': 0
                }
            return True
            
        except Exception as e:
            with self.lock:
                current = self.health_status[host]
                self.health_status[host] = {
                    'healthy': False,
                    'last_check': time.time(),
                    'consecutive_failures': current.get('consecutive_failures', 0) + 1,
                    'last_error': str(e)
                }
            return False
    
    def get_healthy_hosts(self, hosts: list, port: int, max_age: int = 60) -> list:
        """Get list of currently healthy hosts"""
        healthy_hosts = []
        current_time = time.time()
        
        for host in hosts:
            with self.lock:
                status = self.health_status.get(host, {})
                last_check = status.get('last_check', 0)
                
                # If status is too old, recheck
                if current_time - last_check > max_age:
                    # Release lock for the health check
                    pass
                
            # Check health (this may update status)
            if self.check_host_health(host, port):
                healthy_hosts.append(host)
        
        return healthy_hosts


class RetryableOperation:
    """
    Wrapper for operations that need retry logic with circuit breaker
    """
    
    def __init__(self, operation_name: str, max_retries: int = 5,
                 backoff: Optional[ExponentialBackoff] = None,
                 circuit_breaker: Optional[CircuitBreaker] = None):
        self.operation_name = operation_name
        self.max_retries = max_retries
        self.backoff = backoff or ExponentialBackoff()
        self.circuit_breaker = circuit_breaker or CircuitBreaker(name=operation_name)
    
    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with retry logic and circuit breaker"""
        
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                if self.circuit_breaker:
                    return self.circuit_breaker.call(func, *args, **kwargs)
                else:
                    return func(*args, **kwargs)
                    
            except Exception as e:
                last_exception = e
                
                if attempt < self.max_retries - 1:
                    delay = self.backoff.get_delay(attempt)
                    logging.warning(logf(
                        "Operation failed, retrying with backoff",
                        operation=self.operation_name,
                        attempt=attempt + 1,
                        max_attempts=self.max_retries,
                        error=str(e),
                        backoff_seconds=f"{delay:.2f}"
                    ))
                    time.sleep(delay)
                else:
                    logging.error(logf(
                        "Operation failed after all retries",
                        operation=self.operation_name,
                        total_attempts=self.max_retries,
                        final_error=str(e)
                    ))
        
        raise last_exception


# Global instances for common operations
MOUNT_CIRCUIT_BREAKER = CircuitBreaker(failure_threshold=3, recovery_timeout=120, name="mount_operations")
SOCKET_CIRCUIT_BREAKER = CircuitBreaker(failure_threshold=5, recovery_timeout=60, name="socket_operations")
HEALTH_CHECKER = HealthChecker()

# Common backoff patterns
FAST_BACKOFF = ExponentialBackoff(base_delay=1.0, max_delay=30.0)
SLOW_BACKOFF = ExponentialBackoff(base_delay=5.0, max_delay=300.0)
DISK_BACKOFF = ExponentialBackoff(base_delay=2.0, max_delay=120.0, backoff_factor=1.5)


def with_retries(operation_name: str, max_retries: int = 5, 
                backoff_type: str = "fast") -> Callable:
    """Decorator for adding retry logic to functions"""
    
    backoff_map = {
        "fast": FAST_BACKOFF,
        "slow": SLOW_BACKOFF,
        "disk": DISK_BACKOFF
    }
    
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            retry_op = RetryableOperation(
                operation_name=operation_name,
                max_retries=max_retries,
                backoff=backoff_map.get(backoff_type, FAST_BACKOFF)
            )
            return retry_op.execute(func, *args, **kwargs)
        return wrapper
    return decorator


def get_circuit_breaker_stats() -> Dict[str, Dict]:
    """Get statistics for all circuit breakers"""
    return {
        "mount_operations": {
            "state": MOUNT_CIRCUIT_BREAKER.state.value,
            "failure_count": MOUNT_CIRCUIT_BREAKER.failure_count,
            "success_count": MOUNT_CIRCUIT_BREAKER.success_count
        },
        "socket_operations": {
            "state": SOCKET_CIRCUIT_BREAKER.state.value,
            "failure_count": SOCKET_CIRCUIT_BREAKER.failure_count,
            "success_count": SOCKET_CIRCUIT_BREAKER.success_count
        }
    }

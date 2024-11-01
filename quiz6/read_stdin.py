#!/usr/bin/env python3

import hashlib
import sys
import time

class HyperLogLog:
    def __init__(self, num_registers=1024):
        self.num_registers = num_registers
        self.registers = [0] * num_registers
        self.alpha = 0.7213 / (1 + 1.079 / num_registers)

    def _hash(self, item):
        # hash the item passed and return a 64-bit binary string
        h = hashlib.sha1(item.encode()).hexdigest()
        return bin(int(h, 16))[2:].zfill(64)
    
    def add(self, item):
        binary = self._hash(item)

        # the first 10 bits for register index
        index = int(binary[:10], 2)

        # count leading zeros
        count = len(binary[10:].split('1', 1)[0]) + 1

        self.registers[index] = max(self.registers[index], count)

    def estimate(self):
        harmonic_mean = sum([2 ** -r for r in self.registers]) ** -1
        return round(self.alpha * self.num_registers ** 2 * harmonic_mean)
    
# track estimated user count over time
hll = HyperLogLog()
timestamps = []
counts = []

start_time = time.time()

for line in sys.stdin:
    user_id = line.split(" ")
    user_id = user_id[4]
    print("user_id: ", user_id)
    hll.add(user_id)

    # continuously print estimates
    elapsed_time = time.time() - start_time
    if int(elapsed_time) > len(timestamps):
        timestamps.append(elapsed_time)
        counts.append(hll.estimate())
        print(f"Time elapsed: {elapsed_time}s, Estimated unique users: {counts[-1]}")

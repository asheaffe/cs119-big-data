import hashlib

class BloomFilter:

    def __init__(self, m, k):
        self.size = m    # size of bit array
        self.func_num = k   # number of hash functions
        self.bit_array = [0] * self.size  # array of m bits

    def add(self, item):
        hash_value = None

        # compute the ith hash and map it to the bit array
        for i in range(self.func_num):
            # use the hashlib python library to generate a hash value
            hash_value = int(hashlib.md5(item.encode()).hexdigest(), 16) % self.size
            self.bit_array[hash_value] = 1

    def contains(self, item):
        # lookup function for BloomFilter
        for i in range(self.func_num):
            hash_value = int(hashlib.md5(item.encode()).hexdigest(), 16) % self.size
            if self.bit_array[hash_value] == 0:
                return False
        return True
    
    def to_bytes(self):
        # convert from bits to bytes
        bit_str = ''.join(str(bit) for bit in self.bit_array)

        byte_array = int(bit_str, 2).to_bytes((len(bit_str) + 7) // 8, byteorder='big')
        return byte_array
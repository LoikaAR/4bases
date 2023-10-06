import pandas as pd

path = '../esempio_dati/CARDIOPRO-CQ1-AA33/CARDIOPRO-CQ1-AA33.vcf'

with open(path, 'r') as file_:
     for i, line in enumerate(file_):
         if line.startswith('#CHROM'):
             header = line.strip('#').strip().split('\t')
             break


data = pd.read_csv(path, sep ='\t', skiprows=i+1, header=None)

data.columns = header

target_data = data[['CHROM', 'POS', 'REF', 'ALT']]
print(target_data)

def hash_fn(in_str):
    primes = [2, 3, 5, 7, 11, 13, 17]
    res = 1
    for i in range(0, len(in_str)):
        if in_str[i].isdigit():
            res += int(in_str[i])+1 * primes[i % 7]
        else:
            res += ord(in_str[i])+1 * primes[i % 7]
    return res
    

H1 = "chr116272250AG"
H2 = "chr116272250AG"
H3 = "chr116272250AG"

print(f"hash1: {hash_fn(H1)}")
print(f"hash2: {hash_fn(H2)}")
print(f"hash3: {hash_fn(H3)}")

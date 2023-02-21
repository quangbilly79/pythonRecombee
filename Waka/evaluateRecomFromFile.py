import ast

with open('debugRecom.txt', 'r') as file:
    lines = file.readlines()

totalPrecision = 0
totalAveragePrecision = 0
for line in lines:
    id, values = line.strip().split(':', 1)
    values = ast.literal_eval(values)
    totalPrecision += values['precision']
    totalAveragePrecision += values['averagePrecision']

MP = totalPrecision/15612*100
MAP = totalAveragePrecision/15612*100

print(f"MP = {MP:.2f}")
print(f"MAP = {MAP:.2f}")
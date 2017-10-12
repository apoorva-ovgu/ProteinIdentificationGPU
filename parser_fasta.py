aminoacids = ["A","B","C","D","E","F","G","H","I","K","L","M","N","O","P","Q","R","S","T","U","V","W","Y","Z","X"]
aaWeights = [89.09,132.61,121.15,133.1,147.13,165.19,75.07,155.16,131.17,146.19,131.17,149.21,132.12,0.1,115.13,146.15,
             174.2,105.09,119.12,0.1,117.15,204.23,181.19,146.64,128.16]
###unknown = 0.1
mass_dict = {}
for i in range(len(aminoacids)):
    mass_dict[aminoacids[i]] = aaWeights[i]

peptide = "MNSYFTNPSLSCHLAGGQDVLPNVALNSTAYDPVRHFSTYGAAVAQNRIYSTPFYSPQEN\
VVFSSSRGPYDYGSNSFYQEKDMLSNCRQNTLGHNTQTSIAQDFSSEQGRTAPQDQKASI\
QIYPWMQRMNSHSGVGYGADRRRGRQIYSRYQTLELEKEFHFNRYLTRRRRIEIANALCL\
TERQIKIWFQNRRMKWKKESNLTSTLSGGGGGAAADSLGGKEEKREETEEEKQKE"

sum = 0
total = 0
for molecule in peptide:
    print([molecule],mass_dict[molecule])
    sum+=mass_dict[molecule]
    total+=1

#Formula: sum - molecule of water * (no. of residues - 1)
print(sum-18.015*(total-1))
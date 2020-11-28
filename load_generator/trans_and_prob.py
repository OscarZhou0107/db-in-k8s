trans_index = {     
    "init": 0,
    "admc": 1,
    "admr": 2,
    "bess": 3,
    "buyc": 4,
    "buyr": 5,
    "creg": 6,
    "home": 7,
    "newp": 8,
    "ordd": 9,
    "ordi": 10,
    "prod": 11,
    "sreq": 12,
    "sres": 13,
    "shop": 14
}

trans = trans_index.keys()

probMix1 = {}
probMix2 = {}
probMix3 = {}

def init:
    for i in range(len(trans)):
        probMix1[trans[i]] = {}
        probMix2[trans[i]] = {}
        probMix3[trans[i]] = {}
        for j in range(len(tras)):
            probMix1[trans[i]][trans[j]] = 0
            probMix2[trans[i]][trans[j]] = 0
            probMix3[trans[i]][trans[j]] = 0

def popProbMix1:
    pass

def popProbMix2:
    pass

def popProbMix3:
    pass
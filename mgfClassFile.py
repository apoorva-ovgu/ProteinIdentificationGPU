class mgfClass:

    def __init__(self, name, match, score, remainingComparisons, timeReqd):
        self.name = name
        self.match = match
        self.score = float(score)
        self.remainingComparisons = remainingComparisons
        self.timeReqd = timeReqd

        #print("Instance for ",name, " created. FASTA=",match," Score= ",score)
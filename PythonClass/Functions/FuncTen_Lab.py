import random
def roll_dice(sides, numDice):
    for die in range(numDice):
        rng = random.randint(1,sides)
        print "Die #" + str(die + 1) + " rolled a " + str(rng) + "."


roll_dice(6, input("Enter a number"))
print "That's all"
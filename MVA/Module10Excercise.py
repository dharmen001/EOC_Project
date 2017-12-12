guests = []
name = " "

while name != "Done":
    name = input("Enter a name of Guest(Enter Done when invited guest are come:)")
    if name != "Done":
        guests.append(name)
guests.sort()
for guests in guests:
    print (guests)



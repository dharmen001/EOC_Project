import turtle
numoflines = 0
lenghtoflines = 100
usernooflines = int(input("number of lines:>>"))
for steps in range(0,usernooflines):
    turtle.forward(lenghtoflines)
    turtle.left(360/usernooflines)
    for sides in range(0,usernooflines):
        turtle.forward(lenghtoflines/2)
        turtle.left(360/usernooflines)

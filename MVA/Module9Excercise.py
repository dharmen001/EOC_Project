import turtle
newPenColor = "Black"
newLineLength = 0
penColor = input("Enter a Pen color:>>")
lineLength = int(input("Enter a line Lenght:>>"))
angle = int(input("Enter a angle:>>"))
line = int(input("line:>>"))
while line>=0:
    if line == 0:
        print("I am stopping to drawing as completed %d" %line)
    turtle.forward(lineLength)
    turtle.right(angle)
    turtle.color(penColor)
    line = line-1





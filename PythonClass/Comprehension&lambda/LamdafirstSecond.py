def farenheit(temp1,temp2):
    conv_celtofen = (float(9)/5*temp1+32)
    conv_fentocel = (temp2-32* (float (5/9)))
    return conv_celtofen,conv_fentocel
#res = farenheit(36.5,37,37.5,39)
temp1 = (36.5,37,37.5,39)
temp2 = (-50,-10,20,98.6)
F = map(farenheit,temp1,temp2)
print F

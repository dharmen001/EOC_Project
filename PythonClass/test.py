list_var = [1,23,4,56,69]
x = int(input('please enter any no.\n'))

isExit=False
for i in list_var:
    if i == x:
        isExit=True
        break

if isExit:
    print('exist')
else:
    print('not exist')


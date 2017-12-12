"""def bigger_number(x,y):
    #x = input("Enter the first number")
    #y = input("Enther the second number")
    if x>y:
        return x
    else:
        return y

result = bigger_number(input("Enter the first number"), input("Enter the second number"))
print result"""

import sys


def sqrt(n):
    start = 0
    end = n
    m = 0
    min_range = 0.0000000001;

    while end - start > min_range:
        m = (start + end) / 2.0;
        pow2 = m * m
        if abs ( pow2 - n ) <= min_range:
            return m
        elif pow2 < n:
            start = m
        else:
            end = m

    return m


def main():
    for line in sys.stdin:
        n = int ( line )
        print sqrt ( n )


if __name__ == '__main__':
    main ()
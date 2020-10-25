from random import randint
import time
import configparser as cp, sys

"""
This is use for create 30 file one by one in each 5 seconds interval. 
These files will store content dynamically from 'lorem.txt' using below code
"""


def main():
    props = cp.RawConfigParser()
    props.read('src/main/resources/application.properties')
    env = sys.argv[1]
    inputBaseDir = props.get(env, 'input.base.dir')
    inputBaseDir + '/orders'
    a = 1


    # reading content from input file
    with open(inputBaseDir+'/'+'input-file-10000.txt', 'r') as file:
        lines = file.readlines()
        while a <= 30:
            totalline = len(lines)
            # we generate a random number in the range of the number of lines of our input file
            linenumber = randint(0, totalline - 10)
            with open(inputBaseDir+'/log/input-file-{}.txt'.format(a), 'w') as writefile:
                writefile.write(' '.join(line for line in lines[linenumber:totalline]))
            print('creating file log{}.txt'.format(a))
            a += 1
            time.sleep(5)


if __name__ == '__main__':
    main()
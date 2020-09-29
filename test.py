# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 17:25:31 2020

@author: OF
"""
def hello_world():
    print('hello world')



def main():
    hello_world()


if __name__ == '__main__':
    print('This process will take about 3 hours the first time it runs (depending on the server response times and the user\'s processor speed): ')
    tmp = input('Press enter key to continue')
    main()
    tmp = input('Press enter key to exit')
    
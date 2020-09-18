from cx_Freeze import setup, Executable

setup(name = 'multithreading', 
      version = '0.1', 
      description = '',
      executables = [Executable("multithreading.py")])
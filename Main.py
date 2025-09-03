import subprocess

print('Beggining extraction phase')

Extraction = subprocess.run(["python", "Scripts\Extraction.py"], capture_output=True, text=True)
print(Extraction.stdout)

print('Beggining transforming phase')

Transform = subprocess.run(["python", "Scripts\Transform.py"], capture_output=True, text=True)
print(Transform.stdout)

print('Beggining loading phase')

Load = subprocess.run(["python", "Scripts\Load.py"], capture_output=True, text=True)
print(Load.stdout)

print('ETL proccess completed successfully')
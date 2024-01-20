import re
import subprocess
import time

# Предполагая, что у вас есть содержимое документа pretty.html, можно прочитать его следующим образом:
async def parse(inputFile):
    #print(inputFile)
    with open('input.txt', 'w') as file:
         # Записываем содержимое переменной inputFile в файл input.txt
         file.write(inputFile)
    subprocess.run(["tomita-parser", "config.proto"], check=True)
    time.sleep(2)
    with open('output.txt', 'r', encoding='utf-8') as file:
              content = file.read()
        # Используем регулярное выражение для извлечения фамилий и мест в фигурных скобках
    pattern = r'\{[^}]+}'
    matches = re.findall(pattern, content)

    places = []
    persons = []

    for item in matches:
        match = re.search(r'(\w+)\s*=\s*([^\n]+)', item)
        if match:
           key = match.group(1)
           value = match.group(2)
           if key == 'Place':
              places.append(value)
           elif key == 'Person':
              persons.append(value)
    #await print("Places")
    #await print(places)
    #await print("Person")
    #await print(persons)
    return persons, places

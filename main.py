import csv
import glob
import os
import random
import time
import uuid
import zipfile
import multiprocessing as mp
from xml.etree.ElementTree import Element, SubElement, ElementTree, fromstring


def create_random_xml(file_name):
    root = Element('root')

    var1 = SubElement(root, "var")
    var1.set('name', 'id')
    var1.set('value', uuid.uuid4().hex)
    var2 = SubElement(root, "var")
    var2.set('name', 'level')
    var2.set('value', str(random.randint(0, 100)))

    objects = SubElement(root, "objects")
    for _ in range(random.randint(1, 10)):
        obj = SubElement(objects, 'object')
        obj.set('name', uuid.uuid4().hex)

    tree = ElementTree(root)
    tree.write(f'{file_name}.xml')


def create_zip_archive(zip_name, file_names):
    zf = zipfile.ZipFile(f'{zip_name}.zip', mode='w')
    for file in file_names:
        zf.write(file)
    zf.close()


def write_csv(file_name, line):
    with open(file_name, 'a') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(line)


def create_csv_from_zip(zip_name):
    archive = zipfile.ZipFile(zip_name, 'r')
    for file in archive.filelist:
        current_xml = fromstring(archive.read(file.filename))

        var = current_xml.findall('var')
        var_id = var[0].get('value')
        level = var[1].get('value')

        objects = current_xml.find('objects').findall('object')
        write_csv('id_level.csv', [var_id, level])
        for obj in objects:
            write_csv('id_obj_name.csv', [var_id, obj.get('name')])


def func_time(func):
    def timed(*args, **kw):
        start = time.time()
        func(*args, **kw)
        end = time.time()
        print(end - start)

    return timed


@func_time
def main():
    for n in range(50):
        for i in range(1, 101):
            create_random_xml(str(i))
        create_zip_archive(f'zip_{n}', [f'{i}.xml' for i in range(1, 101)])

    with mp.Pool(mp.cpu_count()) as p:
        p.map(create_csv_from_zip, [f'zip_{n}.zip' for n in range(50)])

    for f in glob.glob('*.xml'):
        os.remove(f)


if __name__ == '__main__':
    main()

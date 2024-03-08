import os


def get_last_line(filename):
    with open(filename, 'rb') as file:
        # from https://stackoverflow.com/questions/46258499/how-to-read-the-last-line-of-a-file-in-python
        # use seek to move straight to end of binary file
        file.seek(-2, os.SEEK_END)
        # check line for new line - loop moves up a line and breaks if the line returns a new line only
        while file.read(1) != b'\n':
            file.seek(-2, os.SEEK_CUR)
        last_line = file.readline().decode()
    return last_line



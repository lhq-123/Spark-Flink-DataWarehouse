import logging
import os
import shutil
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def load_input_data(arg):

    args = arg[1].split("~")
    # 源文件路径  "/home/file.txt"
    from_path = args[0]
    # 目标文件路径  "/home/copy_file.txt"
    to_path = args[1]

    if os.path.exists(from_path):
        shutil.copyfile(from_path, to_path)
        logger.info("将原始数据拷贝到指定路径下:" + to_path)
        return True
    else:
        logger.error("原始数据不存在,请检查" + from_path)
        return False


if __name__ == '__main__':
    load_input_data(sys.argv)

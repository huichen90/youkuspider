def dts2ts(datestr):
    '''datestring translate to timestamp'''
    import time
    if len(datestr) == 10:
        timeArray = time.strptime(datestr, "%Y-%m-%d")
        timeStamp = int(time.mktime(timeArray))
        return timeStamp
    else:
        timeArray = time.strptime(datestr, "%Y%m%d")
        timeStamp = int(time.mktime(timeArray))
        return timeStamp


if __name__ == '__main__':
    import re
    import time
    datestr = "nfkljsdf2337/45/45bklq23j52njkln72018年3月24日"
    datestr = re.search(r"(\d{4}年\d{1,2}月\d{1,2}日)", datestr).group(0)  # 利用正则表达式将日期准确提取出来
    print(datestr)
    timeArray = time.strptime(datestr, "%Y年%m月%d日")
    timeStamp = int(time.mktime(timeArray))
    print(timeStamp)


import json
import requests
import hashlib


class Translate(object):
    def __init__(self, q):
        self.url = 'http://api.fanyi.baidu.com/api/trans/vip/translate'

        self.q = q
        self.from1 = 'auto'
        self.to = 'zh'
        self.appid = 20180607000173223
        self.salt = 123

        '''生成秘钥sign'''
        self.pwd = 'Gy4Mzn0ILDyD9fS3iuzf'
        str1 = str(self.appid)+self.q+str(self.salt)+self.pwd
        m = hashlib.md5()
        str1 = str1.encode(encoding='UTF-8', errors='strict')
        m.update(str1)
        self.sign = m.hexdigest()

    def translate(self):
        params = {'q': self.q, 'from': self.from1, 'to': self.to, 'appid': self.appid,
                  'salt': self.salt, 'sign': self.sign}

        response = requests.get(url=self.url, params=params)
        result = response.text
        result = json.loads(result)
        language_table = {'zh': '中文', 'en': '英语', 'yue': '粤语', 'wyw': '文言文', 'jp': '日语', 'kor': '韩语', 'fra': '法语',
                          'spa': '西班牙语', 'th': '泰语', 'ara': '阿拉伯语', 'ru': '俄语', 'pt': '葡萄牙语', 'de': '德语',
                          'it': '意大利语', 'el': '希腊语', 'nl': '荷兰语', 'pl': '波兰语', 'bul': '保加利亚语', 'est': '爱沙尼亚语',
                          'dan': '丹麦语', 'fin': '芬兰语', 'cs': '捷克语', 'rom': '罗马尼亚语', 'slo': '斯洛文尼亚语', 'swe': '瑞典语',
                          'hu': '匈牙利语', 'cht': '繁体中文', 'vie': '越南语'}

        return result['trans_result'][0]['dst'], language_table.get(result['from'])


if __name__ == '__main__':
    t = Translate(q='hello world！')
    rsl, language = t.translate()
    print(rsl, language)

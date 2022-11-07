CONFIG = {
    'log': {
        'level': 'DEBUG',   # 与log库的level一致，包括DEBUG, INFO, ERROR
                            #   DEBUG   - Enable stdout, file, mail （如果在dest中启用）
                            #   INFO    - Enable file, mail         （如果在dest中启用）
                            #   ERROR   - Enable mail               （如果在dest中启用）
        'dest': {
            'stdout': 1, 
            'file': 1, 
            'mail': 1       # 0     - 不使用； 
                            # 1     - 使用，收件人使用mail中设置的to；
                            # 字符串 - 直接指定收件人， Ex. : 'Henry TIAN <chariothy@gmail.com>'
        },
        'sql': 1
    },
    'mail': {
        'from': 'Henry TIAN <15050506668@163.com>',
        'to': 'Henry TIAN <6314849@qq.com>'
    },
    'smtp': {
        'host': 'smtp.163.com',
        'port': 25,
        'user': '15050506668@163.com',
        'pwd': '123456'
    },
    'mysql': {
        'host': 'mysql',
        'port': 3306,
        'db': 'website',
        'user': 'root',
        'pwd': 'pwd'
    },
    'notify': {                         # 通知方式，会对列表中列出的方式进去通知，列表为空则不做任何通知
        'mail': 1,                         # 通过邮件方式通知，需要配置'mail'和'smtp'
        'dingtalk': 1                      # 通过钉钉机器人[http://dwz.win/MqK]通知，需要配置'dingtalk'
    },
    'dingtalk': {                       # 通过钉钉机器人发送通知，具体请见钉钉机器人文档
        'token': 'DINGTALK_BOT_TOKEN',
        'secret' : 'DINGTALK_BOT_SECRET' # 钉钉机器人的三种验证方式之一为密钥验证
    }
}
## 使用方法

1. 只看`era5.py`文件，安装里面对应的库

2. 去ear5网站注册账号，找到自己账号的api信息，复制那两行代码，到 C:\用户\xxx\ 目录创建一个文本文档，复制进去，把文档名字改成 .cdsapirc

3. 运行代码，会先向网站提交申请，然后申请通过后开始下载，注意：

**4. 需要修改的部分：**

路径配置：`install_directory`代表下载文件的存储位置，双斜杠`\\`取消转义

`idm_path`代表你的 idm下载器 exe位置，r""字符串取消转义


**时间范围：自行修改下载年份区间**

## `era5_faster.py`使用方法：
1. 更改路径配置，确认下载目录位置和idm路径
2. 改为按月申请，命名为`xxxx-xx_partial.zip`（自动检测差的天数，不用额外更改文件）
3. 更新了代码逻辑，每次申请前检查下载目录是否有下载了但未成功记录的文件
4. 更新了代码逻辑，在进行一次成功申请后就进行下一次申请，重合了下载时间和申请时间
5. **注意：更改下载年范围 （第234行）**
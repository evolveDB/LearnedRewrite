# REWRITER
改写工具介绍
# Installation
* 安装JAVA环境。早期的JDK版本在Calcite上有编码问题，所以建议使用JDK11以上版本。 点击查看[安装文档](https://www.oracle.com/java/technologies/downloads/#java11)

# 调试
#### 1、代码方式
* 可在Interllij IDEA等IDE中直接运行test。
* 当前测试数据基于TPCH,如需修改为其它测试数据可修改```src/main/schema.json```中json格式的schema
* 修改```src/test```文件中的testSql进行语句测试

#### 2、Server方式
项目中带了HttpServer,可直接运行```nohup /root/jdk-15/bin/java -jar {{输入项目路径}}/rewriter_java.jar --server.port=6336 &```，端口为**6336**
* api: /rewriter POST {sql: "select....", schema: {....}}

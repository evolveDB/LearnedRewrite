
<div align="center">

**An Product for SQL-Rewrite.**

------

<p align="center">
  <a href="#Overview">Overview</a> •
  <a href="#Installation">Installation</a> •
  <a href="## What Can You Do via SQL-Rewriter?">What Can You Do via SQL-Rewriter?</a> •
  <a href="#Datasets">Datasets</a> •
  <a href="#Issues">Issues</a> •
  <a href="#Citation">Citation</a> •
</p>

</div>

![version](https://img.shields.io/badge/version-v1.0.0-blue)

## What's New?

- May 2022: First Commit


## Overview

**SQL-Rewriter**  is a statement rewriter based on [Calcite](https://github.com/apache/calcite). We optimized the logic used by the Calcite rule.


<div align="center">

<img src="https://media-business-card-1258191275.cos.ap-beijing.myqcloud.com/28451654566533_.pic.jpg" width="85%" align="center"/>

</div>

## Installation

**Note: 仅需要安装JAVA环境就可运行项目，早期的JDK版本在Calcite上有编码问题，所以建议使用JDK11以上版本。 点击查看[安装文档](https://www.oracle.com/java/technologies/downloads/#java11)**


## What Can You Do via SQL-Rewriter?

*Use Code*
* 可在Interllij IDEA等IDE中直接运行test。
* 当前测试数据基于TPCH,如需修改为其它测试数据可修改```src/main/schema.json```中json格式的schema
* 修改```src/test```文件中的testSql进行语句测试

![demo](demo_code.gif)

*Use Api*
项目中带了HttpServer,可直接运行```nohup /root/jdk-15/bin/java -jar {{输入项目路径}}/rewriter_java.jar --server.port=6336 &```，端口为**6336**
* api: /rewriter POST {sql: "select....", schema: {....}}

![demo](demo_api.gif)

## Datasets

Now we used TPCH

## Issues
Major improvement/enhancement in future.

* add custom rules

## Citation
Please cite our paper if you use SQL-Rewriter in your work

```bibtex
@article{DBLP:journals/pvldb/ZhouLCF21,
  author    = {Xuanhe Zhou and
               Guoliang Li and
               Chengliang Chai and
               Jianhua Feng},
  title     = {A Learned Query Rewrite System using Monte Carlo Tree Search},
  journal   = {Proc. {VLDB} Endow.},
  volume    = {15},
  number    = {1},
  pages     = {46--58},
  year      = {2021},
}
```
## Contributors

<!-- Copy-paste in your Readme.md file -->

<a href="https://github.com/evolveDB/3-SQL-Rewriter/network/dependencies">
  <img src="https://contrib.rocks/image?repo=evolveDB/3-SQL-Rewriter" />
</a>

We thank all the  contributors to this project, more contributors are welcome!

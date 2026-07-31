# A股融资变化看板 · 云部署手册（小白版）

把看板部署到公网，任何设备打开链接就能看，数据每天自动更新。

---

## 部署后你会得到什么

- 一个公网链接，比如 `https://你的名字-融资看板.streamlit.app`
- 每天 18:30 自动拉取最新融资数据（周一至周五），打开就是最新
- 数据和代码都在你自己的 GitHub 私有仓库里，不公开

---

## 第 0 步：准备（5 分钟）

1. 电脑上装 **Git** —— 你的电脑已经装好了（git 2.54），跳过
2. 注册 GitHub 账号 —— 你说已有账号，登录即可：
   打开 https://github.com/login 登录

---

## 第 1 步：创建 GitHub 私有仓库（5 分钟）

1. 打开 https://github.com/new （如果没登录会先让你登录）
2. 页面从上到下填写：
   - **Repository name**（仓库名）：填 `margin-dashboard`
   - **Description**（描述，可不填）：`A股融资变化看板`
   - **Private / Public**：选 **Private**（私有，重要！代码和数据不公开）
   - 下面 **Add a README file** 之类的选项**全部不要勾**（留空）
3. 点绿色的 **Create repository** 按钮
4. 创建成功后会进入一个新页面，**把浏览器地址栏里的地址复制下来**，类似：
   `https://github.com/你的用户名/margin-dashboard`
   发给我，我来帮你把代码推送上去

---

## 第 2 步：把代码推送到 GitHub（我来做，你只需要授权登录一次）

我执行推送时，电脑会**自动弹出 GitHub 登录窗口**（Git Credential Manager）：

1. 弹出窗口里点 **Sign in with your browser**
2. 浏览器打开后登录你的 GitHub 账号，点 **Authorize**
3. 回到窗口，显示登录成功，就可以关了

推送完成后我会告诉你。

---

## 第 3 步：在 Streamlit 上部署（10 分钟）

1. 打开 https://share.streamlit.io ，点 **Sign in**，用 **GitHub 账号**登录
2. 登录后点右上角绿色的 **New app**
3. 页面有三个下拉框：
   - **Repository**：选 `margin-dashboard`（刚创建的仓库）
   - **Branch**：选 `main`
   - **Main file path**：选 `app.py`
4. 点 **Deploy**，等 3~5 分钟
5. 部署完成后，页面会给你一个链接，类似：
   `https://xxx.streamlit.app`
   —— 这就是你以后每天打开看的地址，可以收藏

> 注意：第一次打开可能显示"数据生成中"，属正常，先做第 4 步。

---

## 第 4 步：首次生成数据（手动触发一次，1 分钟）

首次部署后还没有数据，需要手动触发一次数据更新：

1. 打开仓库页面：`https://github.com/你的用户名/margin-dashboard`
2. 点上方菜单 **Actions**
3. 左侧列表点 **每日数据更新**
4. 右侧点灰色按钮 **Run workflow**，弹出小框后再点绿色的 **Run workflow**
5. 等它跑完（首次要拉 45 天数据，约 30~60 分钟，页面黄色转盘变绿勾）
6. 跑完后回到看板链接刷新，数据就出来了

之后每天 18:30 它会自动跑，**不用你再管**。

---

## 第 5 步：日常使用

- 每天打开看板链接即可，数据是最新的
- 想手动更新：随时去 GitHub → Actions → Run workflow
- 数据是公开的融资融券数据，任何人都能看，无合规问题

---

## 常见问题

| 问题 | 解决 |
|---|---|
| 打开看板显示"数据生成中" | 还没触发过第 4 步，或 Actions 还在跑 |
| 数据停留在几天前 | 去 Actions 看最近的运行是否失败，点进去看红色报错 |
| 想增加/更换指数 | 更新 `指数权重/` 文件夹里的 CSV，push 到仓库，Streamlit 会自动重新部署 |
| Actions 每天自动跑会扣钱吗 | 免费，GitHub 每月送 2000 分钟，每天跑一次只用几分钟 |

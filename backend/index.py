from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

import sys
import os
from os import path
sys.path.append(path.dirname(path.dirname(__file__)))

from utils import fin

from macro import Macro
from sqlalchemy import select

ENV = os.environ.get('ENV', 'dev')
app = FastAPI()
# 设置模板目录
templates = Jinja2Templates(directory="templates")

@app.get('/macro')
async def macro(request: Request, name: str):
    if len(name) == 0:
        return {"message": "name is empty"}
    stmt = select(Macro).where(Macro.name == name.upper())
    with fin.session() as sess:
        # 执行查询
        result = sess.scalars(stmt).one_or_none()
        if result is None:
            return {"message": f"{name} not found"}
        fin.debug(result)
    return result


@app.get("/", response_class=HTMLResponse)
async def read_index(request: Request):
    # 渲染模板并传递请求上下文
    return templates.TemplateResponse("index.html", {"request": request, "api": fin['api']})

# 如果用 FastAPI 内置的命令启动应用:
# uvicorn index:app --reload

if __name__ == "__main__":
    import uvicorn, os
    print(f'Listening ++++++ ENV={ENV} ++++++')
    uvicorn.run("index:app", host="0.0.0.0", port=fin['port'], reload=(ENV != 'prod'))
import json
import time

import requests
from fastapi import FastAPI
from redis_om import get_redis_connection, HashModel
from fastapi.middleware.cors import CORSMiddleware
from starlette.requests import Request
from fastapi.background import BackgroundTasks

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_methods=['*'],
    allow_headers=['*']
)

redis = get_redis_connection(
    host='redis-11617.c16.us-east-1-2.ec2.redns.redis-cloud.com',
    port=11617,
    password='D5tTODZsbbsdUUEIftpJyvXeWvNriNoZ',
    decode_responses=True
)


class Product(HashModel):
    name: str
    price: float
    quantity: int


class Order(HashModel):
    product_id: str
    price: float
    fee: float
    total: float
    quantity: int
    status: str

    class Meta:
        database = redis


@app.post('/orders')
async def create(request: Request, background_tasks: BackgroundTasks):
    global product
    request_example = {"text": "in"}
    body = await request.json()
    req = requests.get('http://localhost:8000/products/%s' % body['id'])
    if req.status_code == 200:
        product = json.loads(req.content.decode('utf-8'))

    order = Order(
        product_id=body['id'],
        price=product['price'],
        fee=0.2 * product['price'],
        total=1.2 * product['price'],
        quantity=body['quantity'],
        status='pending'
    )
    order.save()
    background_tasks.add_task(order_completed, order)

    return order


@app.get('/orders/{pk}')
def get(pk: str):
    return Order.get(pk)


def order_completed(order: Order):
    time.sleep(5)
    order.status = 'completed'
    order.save()
    #after completed the task, now publish message to Redis Stream ro process
    redis.xadd('order_completed', order.dict(), '*')

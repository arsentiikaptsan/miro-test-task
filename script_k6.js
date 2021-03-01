import http from 'k6/http';
import {check} from 'k6';

export let options = {
    stages: [
        {duration: "100s", target: 10},
        // { duration: "1m", target: 0 },
    ],
    ext: {
        loadimpact: {
            projectID: 3525093,
            // Test runs with the same name groups test runs together
            name: "Miro Performance Test"
        }
    }
}

function randomInt() {
    return Math.floor(Math.random() * 100000);
}

function randomWidgetDto(z) {
    return {
        x: randomInt(),
        y: randomInt(),
        width: randomInt(),
        height: randomInt(),
        z: z ? z : null,
    }
}

const baseUrl = 'http://localhost:8080';
const pageSize = 50;

export function setup() {
    for (let i = 0; i < 10000; i++) {
        http.post(`${baseUrl}/widget?tid=${Math.random() + '' + Math.random()}`, JSON.stringify(randomWidgetDto(randomInt())), {
            headers: {'Content-Type': 'application/json; charset=utf-8'},
            tags: {setup: true, name: 'create'}
        });
    }
}

export default function () {
    let responses = http.batch([
        ['GET', `${baseUrl}/widget?size=${pageSize}&from=${randomInt()}`, null, {tags: {name: 'page'}}],
        ['GET', `${baseUrl}/widget_count`, null, {tags: {name: 'count'}}],
    ]);
    check(responses, {
        'status was 200 on count': (r) => r[0].status == 200,
        'status was 200 on page': (r) => r[1].status == 200,
        'result was not negative on count': (r) => r[1].body >= 0
    });
    let isZNull = randomInt() % 7 === 0;
    let response = http.post(`${baseUrl}/widget?tid=${Math.random() + '' + Math.random()}`,
        JSON.stringify(randomWidgetDto(isZNull ? null : randomInt())),
        {
            headers: {'Content-Type': 'application/json; charset=utf-8'},
            tags: {isZNull: isZNull, name: 'create'}
        });
    check(response, {
        'status was 201 on create': (r) => r.status == 201,
        'id is not null on create': (r) => r.json('id')
    });
    let id = response.json('id');
    if (!id) return;
    response = http.get(`${baseUrl}/widget/${id}`, {tags: {name: 'get'}});
    check(response, {
        'status was 200 on get': (r) => r.status == 200,
    });
    isZNull = randomInt() % 7 === 0;
    let dto = randomWidgetDto(isZNull ? null : randomInt());
    dto.id = id;
    response = http.put(`${baseUrl}/widget/${id}`, JSON.stringify(dto), {
        headers: {'Content-Type': 'application/json; charset=utf-8'},
        tags: {isZNull: isZNull, name: 'update'}
    });
    check(response, {
        'status was 200 on update': (r) => r.status == 200,
    });
    response = http.del(`${baseUrl}/widget/${id}`, null, {tags: {name: 'delete'}});
    check(response, {
        'status was 200 on delete': (r) => r.status == 200
    })
}

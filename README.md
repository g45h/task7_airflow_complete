mkdir ./config ./logs ./plugins

echo -e "AIRFLOW_UID=$(id -u)" > .env

docker-compose up airflow-init

docker-compose up -d
_______________

create a connection: Airflow -> Admin -> Connections -> New Connection:

Name: file_sensor

Type: fs

Extra: {"path": "/opt/airflow/data"}
______________

MongoDB Compass queries:

Топ-5 известных комментариев:

{ thumbsUpCount: -1 }
limit 5

Все записи, где длина поля “content” составляет менее 5 символов:

{ $expr: { $lt: [{ $strLenCP: "$content" }, 5] } }


Средний рейтинг по каждому дню (результат должен быть в виде timestamp type):

$match: {
      score: { $exists: true, $ne: null },
      at: { $exists: true, $ne: null }
    }

$addFields: {
  at: {
    $toDate: "$at"
  }
}

$group: {
  _id: {
    $dateToString: {
      format: "%Y-%m-%d",
      date: "$at"
    }
  },
  averageScore: { $avg: "$score" }
}




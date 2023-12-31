## Работа с Zookeeper
Проверено содержимое корневого каталога и дочерних узлов. Создан и проверен свой узел, получено его значение и выведена метоинформация.
![1](https://github.com/DekartVan/BigData/assets/60447026/7481f6fa-1b72-4f64-a639-fbb6c738ed49)

Изменено содержимое созданного узла. Проверена обновлённая метоинформация.
![2](https://github.com/DekartVan/BigData/assets/60447026/41e44d7a-7166-4eae-970f-ba1f62ef4d68)

Созданы дочерние узлы.

![3](https://github.com/DekartVan/BigData/assets/60447026/a75cb46b-4d23-4204-95e3-f5890836fcb6)

Был создан новый узел /mygroup. Через новые консоли были созданы эфимерные узлы.
Из основной консоли было проверено содержимое узла.
![4](https://github.com/DekartVan/BigData/assets/60447026/bdc024ad-5096-4c17-af1f-419c53880baf)

![5](https://github.com/DekartVan/BigData/assets/60447026/eb66de79-5976-49f9-adbd-79ca805d2c84)

## Философы

Проблема обедающих философов возникает, когда несколько философов собираются вокруг круглого стола с вилками - каждый философ должен взять две вилки, чтобы есть, но вилки разделяются между соседними философами. Это может привести к взаимоблокировке, когда каждый философ удерживает одну вилку и ждет освобождения другой.

Представим, что все философы представлены в виде потоков, а вилки - в виде мьютексов, которые являются двоичными семафорами. Количество мест = количеству философов. Каждый философ ест и размышляет. В процессе еды создается временный узел в ZooKeeper (zk.create) для регистрации своей голодной фазы. Затем философ пытается захватить семафоры левой и правой вилки (leftFork.acquire() и rightFork.acquire()). Если оба семафора доступны, философ берет вилки и начинает есть, выполняя задержку в течение случайного времени. После того, как философ закончил есть, он освобождает вилки, вызывая методы rightFork.release() и leftFork.release(). Затем философ вызывает метод think(), внутри которого он удаляет свой временный узел из ZooKeeper (zk.delete) и выполняет задержку, представляющую фазу размышления.

## Двухфазный коммит

Сначала создается экземпляр класса Coordinator с определенными параметрами для контроля над двумяфазным коммитом. Затем запускается поток координатора (coordinatorThread), который вызывает метод run() в классе Coordinator. После этого создается и запускается массив потоков рабочих процессов (workers), количество которых указано в numWorkers.

Каждый поток рабочего процесса (Worker) устанавливает связь с ZooKeeper, используя указанный хост и порт. Затем он создает путь для своего рабочего процесса, который включает корневой путь и идентификатор рабочего процесса (workerPath).

Метод run() в классе Worker выполняется в каждом рабочем процессе. Он генерирует случайное значение "commit" или "abort" для голосования.

Пока корневой узел не появляется в ZooKeeper, процесс ожидает 5 секунд. Это необходимо для того, чтобы убедиться, что координатор уже был создан и готов принять голоса.

Далее рабочий процесс голосует, отправляя сообщение в консоль, которое указывает его идентификатор и выбранное значение голоса. Затем рабочий процесс создает временный узел в ZooKeeper с использованием своего пути и выбранного значения голоса в виде массива байтов.

После создания узла рабочий процесс ожидает 10 секунд (это значение можно изменить), чтобы имитировать выполнение действий, связанных с фазой коммита или отмены. Наконец, рабочий процесс закрывает связь с ZooKeeper.

Таким образом, координатор и рабочие процессы совместно реализуют двухфазный коммит протокол, где координатор принимает голоса рабочих процессов, а затем принимает решение о коммите или отмене на основе полученных голосов.

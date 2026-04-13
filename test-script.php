<?php

final class QuicheThread extends \pmmp\thread\Thread
{
    public function __construct(
        public readonly int $descriptorId,
        public bool $shutdown
    ) { }

    public function run(): void
    {
        $socket = new \NetherGames\Quiche\SocketAddress("127.0.0.1", 19132);
        $config = new \NetherGames\Quiche\Config("cert.pem", "key.pem" );
        $config->setAlpn(["rquic"]);
        $config->setVerifyPeer(false);

        $socket = new \NetherGames\Quiche\QuicheServerSocket([$socket], $config, $this->descriptorId);
        while(!$this->shutdown) {
            $socket->tick();

            print "OK TICK" . PHP_EOL;
        }

        $socket->close();
        print "OK EXIT" . PHP_EOL;
    }
}

// Create a file descriptor id from the main thread
$descriptorId = network_eventfd_create();

// Pass the id to the thread and let ext-quiche create an fd to process it
$t = new QuicheThread($descriptorId, false);
$t->start(\pmmp\thread\Thread::INHERIT_ALL);

sleep(10);
$t->shutdown = true;
network_eventfd_signal($descriptorId);
print "OK" . PHP_EOL;

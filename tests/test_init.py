class MusicPlayer(object):
    instance = None
    init_flag = False

    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super().__new__(cls)

        return cls.instance

    def __init__(self):
        if MusicPlayer.init_flag:
            return
        print('播放器初始化方法__init__')
        MusicPlayer.init_flag = True
    pass


player1 = MusicPlayer()
print(player1)
player2 = MusicPlayer()
print(player2)

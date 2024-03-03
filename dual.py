import random

# Функция для рассчета победителя дуэли
async def duel(player1_luck, player2_luck):
    player1_roll = random.randint(1, player1_luck)
    player2_roll = random.randint(1, player2_luck)

    if player1_roll == player2_roll:
        return 3
    elif player1_roll > player2_roll:
        return 1
    else:
        return 2

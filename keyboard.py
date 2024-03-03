from vkbottle import Keyboard, KeyboardButtonColor, Callback, OpenLink

keyboard_brak = (
    Keyboard(one_time=False, inline=True)
    .add(Callback("💌 Принять",  payload={"cmd": "sure"}), color=KeyboardButtonColor.POSITIVE)
    .add(Callback("💔 Отказать", payload={"cmd": "not_sure"}), color=KeyboardButtonColor.NEGATIVE)
    .get_json()
)

keyboard_dyal = (
    Keyboard(one_time=False, inline=True)
    .add(Callback("Принять",  payload={"cmd": "sure_dyal"}), color=KeyboardButtonColor.POSITIVE)
    .add(Callback("Отказаться", payload={"cmd": "not_sure_dyal"}), color=KeyboardButtonColor.NEGATIVE)
    .get_json()
)

keyboard_ii = (
    Keyboard(one_time=False, inline=True)
    .add(OpenLink(label="Купить",  link="https://vk.com/animmon?w=product-215327961_10832862"), color=KeyboardButtonColor.POSITIVE)
    .get_json()
)

keyboard_reward = (
    Keyboard(one_time=False, inline=True)
    .add(Callback("Ежедневная награда",  payload={"cmd": "daily_reward"}), color=KeyboardButtonColor.POSITIVE)
    .get_json()
)

keyboard_atty = (
    Keyboard(one_time=False, inline=True)
    .add(Callback("Еще",  payload={"cmd": "next_atty"}), color=KeyboardButtonColor.POSITIVE)
    .get_json()
)

keyboard_legs = (
    Keyboard(one_time=False, inline=True)
    .add(Callback("Еще",  payload={"cmd": "next_legs"}), color=KeyboardButtonColor.POSITIVE)
    .get_json()
)

keyboard_ass = (
    Keyboard(one_time=False, inline=True)
    .add(Callback("Еще",  payload={"cmd": "next_ass"}), color=KeyboardButtonColor.POSITIVE)
    .get_json()
)

keyboard_tank = (
    Keyboard(one_time=False, inline=True)
    .add(Callback("Еще",  payload={"cmd": "next_tank"}), color=KeyboardButtonColor.POSITIVE)
    .get_json()
)

keyboard_pet = (
    Keyboard(one_time=False, inline=True)
    .add(Callback("Еще",  payload={"cmd": "next_pet"}), color=KeyboardButtonColor.POSITIVE)
    .get_json()
)

keyboard_gry = (
    Keyboard(one_time=False, inline=True)
    .add(Callback("Еще",  payload={"cmd": "next_gry"}), color=KeyboardButtonColor.POSITIVE)
    .get_json()
)

keyboard_tentacles = (
    Keyboard(one_time=False, inline=True)
    .add(Callback("Еще",  payload={"cmd": "next_tentacles"}), color=KeyboardButtonColor.POSITIVE)
    .get_json()
)
import flet as ft


def main(page: ft.Page):
    for i in range(5000):
        page.controls.append(ft.Text(f"line {i}"))

    page.scroll = "always"
    page.update()

ft.app(target=main, view=ft.AppView.WEB_BROWSER)

import time
from io import BytesIO
from PIL import Image
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

EMAIL = '1884618'
PASSWORD = 'sdfsf'
BORDER = 6
INIT_LEFT = 60

class CrackGeetest():
    def __init__(self):
        self.url = 'https://passport.bilibili.com/login'
        self.browser = webdriver.Chrome()
        self.wait = WebDriverWait(self.browser, 30)
        self.email = EMAIL
        self.password = PASSWORD

    def __del__(self):
        self.browser.close()

    def get_geetest_button(self):
        """
        点击登录按钮，出现验证码
        :return:
        """
        button = self.wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "btn-login")))

        return button

    def get_position(self):
        """
        获取验证码位置
        :return: 验证码位置元素
        """
        img = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'geetest_canvas_img')))
        time.sleep(2)
        location = img.location
        size = img.size
        top, bottom, left, right = location['y'], location['y'] + size['height'], location['x'], location['x'] + size['width']
        return (top, bottom, left, right)

    def get_screenshot(self):
        """
        获取网页截图，
        :return:截图对象
        """
        screenshot = self.browser.get_screenshot_as_png() # 获取当前窗口的截图作为二进制数据
        screenshot = Image.open(BytesIO(screenshot))
        return screenshot

    def delete_style(self):
        '''
        执行js脚本，获取无滑块图
        :return None
        '''
        js = 'document.querySelectorAll("canvas")[3].style=""'
        self.browser.execute_script(js)

    def get_slider(self, name='captcha.png'):
        """
        获取滑块
        :return:滑块对象
        """
        slider = self.wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'geetest_slider_button')))
        return slider


    def get_geetest_image(self, name):
        """
        获取验证码图
        :return:图片对象
        """
        top, bottom, left, right = self.get_position()
        print('验证码位置', top, bottom, left, right)
        screenshot = self.get_screenshot()
        captcha = screenshot.crop((left, top, right, bottom))
        captcha.save(name)
        return captcha


    def open(self):
        """
        打开网页输入用户名密码
        :return:
        """
        self.browser.get(self.url)
        email = self.wait.until(EC.presence_of_element_located((By.ID, 'login-username')))
        password = self.wait.until(EC.presence_of_element_located((By.ID, 'login-passwd')))
        email.send_keys(self.email)
        password.send_keys(self.password)

    def get_gap(self, image1, image2):
        """
        获取缺口偏移量
        :param iamge1:不带缺口的图片
        :param iamge2: 带缺口的图片
        :return:
        """
        for i in range(INIT_LEFT, image1.size[0]):
            for j in range(image1.size[1]):
                if not self.is_pixel_equal(image1, image2, i, j):
                    return i
        return INIT_LEFT

    def is_pixel_equal(self, image1, image2, x, y):
        """
        判断两个像素是否相同
        :param image1: 图片1
        :param image2: 图片2
        :param x: 位置1
        :param y: 位置2
        :return: 像素是否相同
        """
        #取两个图片的 像素点
        pixel1 = image1.load()[x, y]
        pixel2 = image2.load()[x, y]
        threshold = 60
        if abs(pixel1[0] - pixel2[0] < threshold and abs(pixel1[1] - pixel2[1] < threshold and abs(
            pixel1[2] - pixel2[2] < threshold
        ))):
            return True
        else:
            return False

    def get_track(self, distance):
        '''
        根据偏移量获取移动轨迹
        :param distance: 偏移量
        :return: 移动轨迹
        '''
        # 移动轨迹
        track = []
        # 当前位移
        current = 0
        # 减速阈值
        mid = distance * 3 / 5
        # 计算间隔
        t = 0.2
        # 初速度
        v = 0
        # 滑超过过一段距离
        distance += 15
        while current < distance:
            if current < mid:
                # 加速度为正
                a = 1
            else:
                # 加速度为负
                a = -0.5
            # 初速度 v0
            v0 = v
            # 当前速度 v
            v = v0 + a * t
            # 移动距离 move-->x
            move = v0 * t + 1 / 2 * a * t * t
            # 当前位移
            current += move
            # 加入轨迹
            track.append(round(move))
        return track


    def shake_mouse(self):
        '''
        模拟人手释放鼠标时的抖动
        :return: None
        '''
        ActionChains(self.browser).move_by_offset(xoffset=-2, yoffset=0).perform()
        ActionChains(self.browser).move_by_offset(xoffset=1, yoffset=0).perform()

    def move_to_gap(self, slider, tracks):
        """
        拖动滑块到缺口
        :param slider:滑块
        :param track: 轨迹
        :return:
        """
        back_tracks = [-1, -1, -2, -2, -3, -2, -2, -1, -1]
        ActionChains(self.browser).click_and_hold(slider).perform()
        # 正向
        for x in tracks:
            ActionChains(self.browser).move_by_offset(xoffset=x, yoffset=0).perform()
        time.sleep(0.5)
        # 逆向
        for x in back_tracks:
            ActionChains(self.browser).move_by_offset(xoffset=x, yoffset=0).perform()
        # 模拟抖动
        self.shake_mouse()
        time.sleep(0.5)
        ActionChains(self.browser).release().perform()

    def login(self):
        """
        登录
        """
        submit = self.wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'btn-login')))
        submit.click()
        time.sleep(10)
        print('登陆成功')

    def crack(self):
        # 输入用户名
        self.open()
        # 点击验证按钮
        button = self.get_geetest_button()
        button.click()
        # 获取验证码图片

        # 点按呼出缺口
        slider = self.get_slider()
        # slider.click()
        # 获取带缺口的验证码图片
        image1 = self.get_geetest_image('captcha1.png')
        self.delete_style()
        image2 = self.get_geetest_image('captcha2.png')
        # 获取缺口位置
        gap = self.get_gap(image2, image1)
        print('缺口位置', gap)
        # 减去缺口位移
        gap -= BORDER
        # 获取移动轨迹
        track = self.get_track(gap)
        print('滑动轨迹', track)
        # 拖动滑块
        self.move_to_gap(slider, track)

        success = self.wait.until(
            EC.text_to_be_present_in_element((By.CLASS_NAME, 'geetest_success_radar_tip_content'), '验证成功'))
        print(success)

        # 失败后重试
        if not success:
            self.crack()
        else:
            self.login()


if __name__ == '__main__':
    crack = CrackGeetest()
    crack.crack()











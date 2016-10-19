import { TeleporterPage } from './app.po';

describe('teleporter App', function() {
  let page: TeleporterPage;

  beforeEach(() => {
    page = new TeleporterPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});

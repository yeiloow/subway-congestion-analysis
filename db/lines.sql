BEGIN TRANSACTION;

-- Insert Lines
INSERT OR IGNORE INTO Lines (line_name, operator, color_hex) VALUES 
('1호선', '서울교통공사/KORAIL', '#0052A4'),
('2호선', '서울교통공사', '#00A84D'),
('3호선', '서울교통공사/KORAIL', '#EF7C1C'),
('4호선', '서울교통공사/KORAIL', '#00A5DE'),
('5호선', '서울교통공사', '#996CAC'),
('6호선', '서울교통공사', '#CD7C2F'),
('7호선', '서울교통공사', '#747F00'),
('8호선', '서울교통공사', '#E6186C'),
('9호선', '서울시메트로9호선', '#BDB092');

COMMIT;

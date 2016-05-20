#! /usr/bin/octave -qf

arg_list = argv();


filename = arg_list{1};
tmp = load("-ascii", filename);

acc = tmp';

for i = 2:nargin
  filename = arg_list{i};
  x = load("-ascii", filename);
  acc = acc + x';
endfor

plot(acc);

pause();

